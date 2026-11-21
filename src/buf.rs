//! Append-only record streams built from fixed-size blocks carved out of a
//! per-thread arena of (transparent huge page) anonymous memory. Pushing never
//! moves data, so a record's (stream, index) stays valid.

use crate::sys;

pub const BLOCK: usize = 1024; // records per block

pub struct Arena {
    cur: usize,
    end: usize,
    chunk: usize,
}

unsafe impl Send for Arena {}

impl Arena {
    pub fn new(chunk: usize) -> Arena {
        Arena { cur: 0, end: 0, chunk: chunk.max(4 << 20) }
    }
    #[inline]
    pub fn alloc(&mut self, bytes: usize) -> *mut u8 {
        let bytes = (bytes + 63) & !63;
        if self.cur + bytes > self.end {
            let size = self.chunk.max(bytes);
            let p = sys::anon(size, true) as usize;
            self.cur = p;
            self.end = p + size;
        }
        let p = self.cur;
        self.cur += bytes;
        p as *mut u8
    }
}

pub struct Stream<T> {
    pub blocks: Vec<*mut T>,
    cur: *mut T,
    end: *mut T,
}

unsafe impl<T> Send for Stream<T> {}
unsafe impl<T> Sync for Stream<T> {}

impl<T: Copy> Stream<T> {
    pub fn new() -> Self {
        Stream { blocks: Vec::new(), cur: std::ptr::null_mut(), end: std::ptr::null_mut() }
    }
    #[cold]
    #[inline(never)]
    fn grow(&mut self, arena: &mut Arena) {
        let b = arena.alloc(BLOCK * std::mem::size_of::<T>()) as *mut T;
        self.blocks.push(b);
        self.cur = b;
        self.end = unsafe { b.add(BLOCK) };
    }
    /// Appends and returns the record's index.
    #[inline(always)]
    pub fn push(&mut self, v: T, arena: &mut Arena) -> usize {
        if self.cur == self.end {
            self.grow(arena);
        }
        unsafe {
            self.cur.write(v);
            self.cur = self.cur.add(1);
        }
        self.len() - 1
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        match self.blocks.last() {
            None => 0,
            Some(&last) => (self.blocks.len() - 1) * BLOCK + (unsafe { self.cur.offset_from(last) }) as usize,
        }
    }
    #[inline(always)]
    pub fn ptr(&self, i: usize) -> *mut T {
        unsafe { self.blocks.get_unchecked(i / BLOCK).add(i % BLOCK) }
    }
    /// Calls `f` on each block's filled part.
    #[inline(always)]
    pub fn for_slices(&self, mut f: impl FnMut(&[T])) {
        let n = self.blocks.len();
        for (k, &b) in self.blocks.iter().enumerate() {
            let len = if k + 1 == n { (unsafe { self.cur.offset_from(b) }) as usize } else { BLOCK };
            f(unsafe { std::slice::from_raw_parts(b, len) });
        }
    }
}
