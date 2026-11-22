//! Thin wrappers over the few libc calls the pipeline needs (no crates).
#![allow(dead_code)]

use std::fs::File;
use std::os::fd::AsRawFd;

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    fn _exit(code: i32) -> !;
}

const PROT_READ: i32 = 1;
const PROT_WRITE: i32 = 2;
const MAP_SHARED: i32 = 1;
const MAP_PRIVATE: i32 = 2;
#[cfg(target_os = "linux")]
const MAP_ANONYMOUS: i32 = 0x20;
#[cfg(not(target_os = "linux"))]
const MAP_ANONYMOUS: i32 = 0x1000;
#[cfg(target_os = "linux")]
const MAP_NORESERVE: i32 = 0x4000;
#[cfg(not(target_os = "linux"))]
const MAP_NORESERVE: i32 = 0x40;
#[cfg(target_os = "linux")]
const MAP_POPULATE: i32 = 0x8000;
#[cfg(not(target_os = "linux"))]
const MAP_POPULATE: i32 = 0;

pub const MADV_SEQUENTIAL: i32 = 2;
pub const MADV_WILLNEED: i32 = 3;
pub const MADV_HUGEPAGE: i32 = 14;

pub fn exit_now(code: i32) -> ! {
    unsafe { _exit(code) }
}

/// A read-only file mapping. Empty files map to an empty slice.
pub struct Mapped {
    pub ptr: *const u8,
    pub len: usize,
}
unsafe impl Send for Mapped {}
unsafe impl Sync for Mapped {}

impl Mapped {
    pub fn open(path: &std::path::Path, populate: bool) -> std::io::Result<(Mapped, File)> {
        let f = File::open(path)?;
        let len = f.metadata()?.len() as usize;
        if len == 0 {
            return Ok((Mapped { ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(), len: 0 }, f));
        }
        let flags = MAP_PRIVATE | if populate { MAP_POPULATE } else { 0 };
        let p = unsafe { mmap(std::ptr::null_mut(), len, PROT_READ, flags, f.as_raw_fd(), 0) };
        if p as isize == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok((Mapped { ptr: p, len }, f))
    }
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
    pub fn advise(&self, advice: i32) {
        if self.len > 0 {
            unsafe { madvise(self.ptr as *mut u8, self.len, advice) };
        }
    }
}

/// Anonymous zeroed memory, lazily committed. Never freed (the process exits).
pub fn anon(len: usize, huge: bool) -> *mut u8 {
    let len = (len + 4095) & !4095;
    if len == 0 {
        return std::ptr::NonNull::<u8>::dangling().as_ptr();
    }
    let p = unsafe {
        mmap(std::ptr::null_mut(), len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0)
    };
    if p as isize == -1 {
        eprintln!("anon mmap of {len} bytes failed: {}", std::io::Error::last_os_error());
        exit_now(3);
    }
    if huge && cfg!(target_os = "linux") {
        unsafe { madvise(p, len, MADV_HUGEPAGE) };
    }
    p
}

/// Unmaps `[addr, addr + len)`; both must be page-aligned.
pub fn unmap(addr: *const u8, len: usize) {
    unsafe { munmap(addr as *mut u8, len) };
}

#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}
extern "C" {
    fn clock_gettime(clk: i32, ts: *mut Timespec) -> i32;
}

/// CPU time used by the calling thread, in milliseconds.
pub fn thread_cpu_ms() -> f64 {
    let mut ts = Timespec { tv_sec: 0, tv_nsec: 0 };
    let clk = if cfg!(target_os = "linux") { 3 } else { 16 };
    unsafe { clock_gettime(clk, &mut ts) };
    ts.tv_sec as f64 * 1e3 + ts.tv_nsec as f64 * 1e-6
}
