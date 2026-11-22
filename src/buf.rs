//! Append-only record streams built from fixed-size blocks carved out of one
//! per-thread arena (a single lazily committed reservation), so a record is
//! addressable by a 32-bit index into its thread's arena.

use crate::sys;

pub struct Arena {
    base: usize,
    used: usize,
    cap: usize,
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    pub fn new(cap: usize, huge: bool) -> Arena {
        let cap = (cap + (2 << 20)) & !((2 << 20) - 1);
        Arena { base: sys::anon(cap, huge) as usize, used: 0, cap }
    }
    /// Gives the memory back. Nothing may point into the arena any more.
    #[allow(dead_code)]
    pub fn free(&mut self) {
        if self.cap > 0 {
            sys::unmap(self.base as *const u8, self.cap);
            self.cap = 0;
            self.used = 0;
        }
    }
    #[inline]
    pub fn alloc(&mut self, bytes: usize) -> *mut u8 {
        let bytes = (bytes + 63) & !63;
        if self.used + bytes > self.cap {
            eprintln!("arena exhausted ({} bytes)", self.cap);
            sys::exit_now(4);
        }
        let p = self.base + self.used;
        self.used += bytes;
        p as *mut u8
    }
    #[inline(always)]
    pub fn base(&self) -> *mut u8 {
        self.base as *mut u8
    }
}

/// `n` streams of `T`, each a list of blocks of `block` records.
pub struct Streams<T> {
    hot: Vec<(*mut T, *mut T)>, // (next slot, end of block)
    blocks: Vec<Vec<*mut T>>,
    block: usize,
}

unsafe impl<T> Send for Streams<T> {}
unsafe impl<T> Sync for Streams<T> {}

impl<T: Copy> Streams<T> {
    pub fn new(n: usize, block: usize) -> Self {
        Streams { hot: vec![(std::ptr::null_mut(), std::ptr::null_mut()); n], blocks: (0..n).map(|_| Vec::new()).collect(), block }
    }
    #[cold]
    #[inline(never)]
    fn grow(&mut self, s: usize, arena: &mut Arena) {
        let b = arena.alloc(self.block * std::mem::size_of::<T>()) as *mut T;
        self.blocks[s].push(b);
        self.hot[s] = (b, unsafe { b.add(self.block) });
    }
    /// Appends to stream `s`; returns where the record went.
    #[inline(always)]
    pub fn push(&mut self, s: usize, v: T, arena: &mut Arena) -> *mut T {
        unsafe {
            if self.hot.get_unchecked(s).0 == self.hot.get_unchecked(s).1 {
                self.grow(s, arena);
            }
            let h = self.hot.get_unchecked_mut(s);
            let p = h.0;
            p.write(v);
            h.0 = p.add(1);
            p
        }
    }
    pub fn len(&self, s: usize) -> usize {
        match self.blocks[s].last() {
            None => 0,
            Some(&last) => (self.blocks[s].len() - 1) * self.block + (unsafe { self.hot[s].0.offset_from(last) }) as usize,
        }
    }
    /// Calls `f` on each block's filled part of stream `s`.
    #[inline(always)]
    pub fn for_slices(&self, s: usize, mut f: impl FnMut(&[T])) {
        let bl = &self.blocks[s];
        let n = bl.len();
        for (k, &b) in bl.iter().enumerate() {
            let len = if k + 1 == n { (unsafe { self.hot[s].0.offset_from(b) }) as usize } else { self.block };
            f(unsafe { std::slice::from_raw_parts(b, len) });
        }
    }
}
