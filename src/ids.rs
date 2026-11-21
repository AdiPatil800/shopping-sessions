//! IDs as 64-bit keys.
//!
//! An ID of at most 9 bytes, all in 0x01..=0x7F, packs exactly into 63 bits,
//! 7 bits a byte, first byte highest, zero padded. That packing is injective
//! and its numeric order is the byte-wise order of the IDs. Every other ID
//! (longer, non-ASCII, or holding a NUL) is interned and gets a key with the
//! top bit set. Key 0 is never produced: IDs are non-empty.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;

pub const SLOW_BIT: u64 = 1 << 63;

struct Interner {
    map: HashMap<Vec<u8>, u32>,
    list: Vec<Vec<u8>>,
}

static INTERNER: Mutex<Option<Interner>> = Mutex::new(None);

#[cold]
#[inline(never)]
fn intern(bytes: &[u8]) -> u64 {
    let mut guard = INTERNER.lock().unwrap();
    let it = guard.get_or_insert_with(|| Interner { map: HashMap::new(), list: Vec::new() });
    if let Some(&i) = it.map.get(bytes) {
        return SLOW_BIT | i as u64;
    }
    let i = it.list.len() as u32;
    it.list.push(bytes.to_vec());
    it.map.insert(bytes.to_vec(), i);
    SLOW_BIT | i as u64
}

/// The key of a non-empty ID. Reads up to 9 bytes from `p` when `len <= 9`,
/// and 8 bytes even when `len < 8`: the caller guarantees 8 readable bytes.
#[inline(always)]
pub unsafe fn id_key(p: *const u8, len: usize) -> u64 {
    debug_assert!(len > 0);
    if len <= 9 {
        let raw = std::ptr::read_unaligned(p as *const u64);
        let m = if len >= 8 { !0u64 } else { (1u64 << (len * 8)) - 1 };
        let lo = raw & m;
        let ninth = if len == 9 { *p.add(8) as u64 } else { 0 };
        let high = (lo & 0x8080_8080_8080_8080) | (ninth & 0x80);
        let x = lo | (!m & 0x0101_0101_0101_0101);
        let zero = x.wrapping_sub(0x0101_0101_0101_0101) & !x & 0x8080_8080_8080_8080;
        let ninth_zero = (len == 9) & (ninth == 0);
        if (high | zero) == 0 && !ninth_zero {
            let mut x = lo.swap_bytes();
            x = ((x & 0x7F00_7F00_7F00_7F00) >> 1) | (x & 0x007F_007F_007F_007F);
            x = ((x & 0x3FFF_0000_3FFF_0000) >> 2) | (x & 0x0000_3FFF_0000_3FFF);
            x = ((x & 0x0FFF_FFFF_0000_0000) >> 4) | (x & 0x0000_0000_0FFF_FFFF);
            return (x << 7) | ninth;
        }
    }
    intern(std::slice::from_raw_parts(p, len))
}

/// Safe variant for slices of any length (copies short ones into a padded buffer).
pub fn id_key_slice(b: &[u8]) -> u64 {
    if b.len() <= 9 {
        let mut buf = [0u8; 16];
        buf[..b.len()].copy_from_slice(b);
        unsafe { id_key(buf.as_ptr(), b.len()) }
    } else {
        intern(b)
    }
}

/// Appends the ID's bytes to `out`.
#[inline]
pub fn key_bytes(key: u64, out: &mut Vec<u8>) {
    if key & SLOW_BIT == 0 {
        let mut shift = 56i32;
        while shift >= 0 {
            let c = ((key >> shift) & 0x7F) as u8;
            if c == 0 {
                break;
            }
            out.push(c);
            shift -= 7;
        }
    } else {
        let guard = INTERNER.lock().unwrap();
        out.extend_from_slice(&guard.as_ref().unwrap().list[(key & !SLOW_BIT) as usize]);
    }
}

#[cold]
#[inline(never)]
fn cmp_slow(a: u64, b: u64) -> Ordering {
    let (mut x, mut y) = (Vec::new(), Vec::new());
    key_bytes(a, &mut x);
    key_bytes(b, &mut y);
    x.cmp(&y)
}

/// Byte-wise order of the IDs behind two keys.
#[inline(always)]
pub fn cmp_key(a: u64, b: u64) -> Ordering {
    if (a | b) & SLOW_BIT == 0 {
        a.cmp(&b)
    } else {
        cmp_slow(a, b)
    }
}
