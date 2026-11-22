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

/// Drops the top bit of each byte: 8 bytes become 56 bits.
#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
fn compress7(x: u64) -> u64 {
    unsafe { std::arch::x86_64::_pext_u64(x, 0x7F7F_7F7F_7F7F_7F7F) }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
#[inline(always)]
fn compress7(mut x: u64) -> u64 {
    x = ((x & 0x7F00_7F00_7F00_7F00) >> 1) | (x & 0x007F_007F_007F_007F);
    x = ((x & 0x3FFF_0000_3FFF_0000) >> 2) | (x & 0x0000_3FFF_0000_3FFF);
    x = ((x & 0x0FFF_FFFF_0000_0000) >> 4) | (x & 0x0000_0000_0FFF_FFFF);
    x
}

/// A mask of the low `len` bytes of a u64 (all of them for `len >= 8`).
#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
fn low_bytes(len: usize) -> u64 {
    unsafe { std::arch::x86_64::_bzhi_u64(!0u64, (len as u32) * 8) }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
#[inline(always)]
fn low_bytes(len: usize) -> u64 {
    if len >= 8 {
        !0u64
    } else {
        (1u64 << (len * 8)) - 1
    }
}

/// The key of a non-empty ID. Reads 9 bytes from `p` whatever `len` is: the
/// caller guarantees they are readable.
#[inline(always)]
pub unsafe fn id_key(p: *const u8, len: usize) -> u64 {
    debug_assert!(len > 0);
    if len <= 9 {
        let raw = std::ptr::read_unaligned(p as *const u64);
        let m = low_bytes(len);
        let lo = raw & m;
        let is9 = (len == 9) as u64;
        let ninth = (*p.add(8) as u64) & 0u64.wrapping_sub(is9);
        // every byte must be 0x01..=0x7F: no high bit, and no zero among the first `len`
        let x = lo | (!m & 0x0101_0101_0101_0101);
        let zero = x.wrapping_sub(0x0101_0101_0101_0101) & !x;
        let ninth_bad = (ninth.wrapping_sub(is9) | ninth) & 0x80; // ninth == 0 when present, or >= 0x80
        if ((lo | zero) & 0x8080_8080_8080_8080) | ninth_bad == 0 {
            return (compress7(lo.swap_bytes()) << 7) | ninth;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip_and_keep_order() {
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut rnd = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabets: [&[u8]; 3] = [b"0123456789abcdefghijklmnopqrstuvwxyz", b"abAB01 \t\x01\x7f", b"ab\x00\x80\xff\xc3\xa9z"];
        let mut all: Vec<(Vec<u8>, u64)> = Vec::new();
        for i in 0..200_000 {
            let al = alphabets[(i % 3) as usize];
            let len = 1 + (rnd() % 12) as usize;
            let s: Vec<u8> = (0..len).map(|_| al[(rnd() % al.len() as u64) as usize]).collect();
            let k = id_key_slice(&s);
            let mut padded = s.clone();
            padded.extend_from_slice(&[0xAA; 16]);
            let k2 = if s.len() <= 9 { unsafe { id_key(padded.as_ptr(), s.len()) } } else { k };
            assert_eq!(k, k2);
            let mut back = Vec::new();
            key_bytes(k, &mut back);
            assert_eq!(back, s);
            all.push((s, k));
        }
        for w in all.windows(2) {
            let (a, b) = (&w[0], &w[1]);
            assert_eq!(a.0 == b.0, a.1 == b.1);
            assert_eq!(cmp_key(a.1, b.1), a.0.cmp(&b.0), "{:?} {:?}", a.0, b.0);
        }
    }
}
