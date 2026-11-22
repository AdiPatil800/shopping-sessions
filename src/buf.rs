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
    /// Bytes handed out so far.
    pub fn used(&self) -> usize {
        self.used
    }
    #[inline(always)]
    pub fn base(&self) -> *mut u8 {
        self.base as *mut u8
    }
}

/// One cache line of pending records for a stream in write-combining mode.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Line([u8; 64]);

/// `n` streams of `T`, each a list of blocks of `block` records. In
/// write-combining mode (16-byte records only) a stream collects four records
/// in a cache line of its own and sends the line to its block with streaming
/// stores, so the block memory is never read into the cache.
pub struct Streams<T> {
    hot: Vec<(*mut T, *mut T)>, // (next slot, end of block)
    blocks: Vec<Vec<*mut T>>,
    block: usize,
    wc: bool,
    lines: Vec<Line>,
    fill: Vec<u8>,
}

unsafe impl<T> Send for Streams<T> {}
unsafe impl<T> Sync for Streams<T> {}

impl<T: Copy> Streams<T> {
    pub fn new(n: usize, block: usize, wc: bool) -> Self {
        let wc = wc && std::mem::size_of::<T>() == 16 && block % 4 == 0;
        Streams {
            hot: vec![(std::ptr::null_mut(), std::ptr::null_mut()); n],
            blocks: (0..n).map(|_| Vec::new()).collect(),
            block,
            wc,
            lines: if wc { vec![Line([0; 64]); n] } else { Vec::new() },
            fill: if wc { vec![0; n] } else { Vec::new() },
        }
    }
    #[cold]
    #[inline(never)]
    fn grow(&mut self, s: usize, arena: &mut Arena) {
        let b = arena.alloc(self.block * std::mem::size_of::<T>()) as *mut T;
        self.blocks[s].push(b);
        self.hot[s] = (b, unsafe { b.add(self.block) });
    }
    /// Appends to stream `s`; returns where the record goes.
    #[inline(always)]
    pub fn push(&mut self, s: usize, v: T, arena: &mut Arena) -> *mut T {
        unsafe {
            if self.wc {
                let f = *self.fill.get_unchecked(s) as usize;
                if f == 0 && self.hot.get_unchecked(s).0 == self.hot.get_unchecked(s).1 {
                    self.grow(s, arena);
                }
                let dst = self.hot.get_unchecked(s).0;
                let line = self.lines.get_unchecked_mut(s) as *mut Line as *mut T;
                line.add(f).write(v);
                if f == 3 {
                    stream_line(line as *const u8, dst as *mut u8);
                    self.hot.get_unchecked_mut(s).0 = dst.add(4);
                    *self.fill.get_unchecked_mut(s) = 0;
                } else {
                    *self.fill.get_unchecked_mut(s) = f as u8 + 1;
                }
                return dst.add(f);
            }
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
    /// Writes out the pending partial lines. Call once, after the last push.
    pub fn finish(&mut self) {
        if !self.wc {
            return;
        }
        for s in 0..self.hot.len() {
            let f = self.fill[s] as usize;
            if f > 0 {
                unsafe {
                    let dst = self.hot[s].0;
                    std::ptr::copy_nonoverlapping(&self.lines[s] as *const Line as *const T, dst, f);
                    self.hot[s].0 = dst.add(f);
                }
                self.fill[s] = 0;
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
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

/// Copies one 64-byte line to a 64-byte-aligned destination, bypassing the cache.
#[inline(always)]
unsafe fn stream_line(src: *const u8, dst: *mut u8) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::*;
        let a = _mm256_load_si256(src as *const __m256i);
        let b = _mm256_load_si256(src.add(32) as *const __m256i);
        _mm256_stream_si256(dst as *mut __m256i, a);
        _mm256_stream_si256(dst.add(32) as *mut __m256i, b);
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        std::ptr::copy_nonoverlapping(src, dst, 64);
    }
}
