//! Timestamps: the three valid forms, to milliseconds since the Unix epoch.

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

#[inline(always)]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn days_in_month(y: i64, m: i64) -> i64 {
    const DIM: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
        29
    } else {
        DIM[m as usize]
    }
}

/// `YYYY-MM-DDTHH:MM:SS` at `p` (19 readable bytes) to seconds since the epoch.
#[inline(always)]
unsafe fn parse_datetime(p: *const u8) -> Option<i64> {
    let d = |i: usize| (*p.add(i)).wrapping_sub(b'0') as i64;
    let digits = [0usize, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18];
    let mut bad = 0i64;
    for &i in &digits {
        bad |= ((d(i) as u64) > 9) as i64;
    }
    bad |= (*p.add(4) != b'-') as i64;
    bad |= (*p.add(7) != b'-') as i64;
    bad |= (*p.add(10) != b'T') as i64;
    bad |= (*p.add(13) != b':') as i64;
    bad |= (*p.add(16) != b':') as i64;
    if bad != 0 {
        return None;
    }
    let year = d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3);
    let month = d(5) * 10 + d(6);
    let day = d(8) * 10 + d(9);
    let hour = d(11) * 10 + d(12);
    let minute = d(14) * 10 + d(15);
    let second = d(17) * 10 + d(18);
    if month < 1 || month > 12 || day < 1 || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    if day > days_in_month(year, month) {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second)
}

/// A `ts` field to milliseconds, or `None` for a bad timestamp. An F3 value too
/// long for the window to matter is reported as `None` too. The caller
/// guarantees `max(len, 25)` readable bytes at `p` only when it says so via
/// `len`: nothing past `len` is read.
#[inline(always)]
pub unsafe fn parse_ts(p: *const u8, len: usize) -> Option<i64> {
    if len == 20 && *p.add(19) == b'Z' {
        return parse_datetime(p).map(|s| s * 1000);
    }
    if len == 25 && (*p.add(19) == b'+' || *p.add(19) == b'-') {
        let secs = parse_datetime(p)?;
        let d = |i: usize| (*p.add(i)).wrapping_sub(b'0') as i64;
        if (d(20) as u64) > 9 || (d(21) as u64) > 9 || (d(23) as u64) > 9 || (d(24) as u64) > 9 || *p.add(22) != b':' {
            return None;
        }
        let oh = d(20) * 10 + d(21);
        let om = d(23) * 10 + d(24);
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60;
        let secs = if *p.add(19) == b'+' { secs - off } else { secs + off };
        return Some(secs * 1000);
    }
    parse_f3(p, len)
}

#[inline(always)]
unsafe fn parse_f3(p: *const u8, len: usize) -> Option<i64> {
    if len == 0 {
        return None;
    }
    let mut v: u64 = 0;
    let mut significant = 0usize;
    for i in 0..len {
        let d = (*p.add(i)).wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if v != 0 || d != 0 {
            significant += 1;
            if significant > 18 {
                // Still all digits? Either way it is outside every window.
                for j in i..len {
                    if (*p.add(j)).wrapping_sub(b'0') > 9 {
                        return None;
                    }
                }
                return None;
            }
        }
        v = v * 10 + d as u64;
    }
    Some(v as i64)
}

pub fn parse_ts_slice(b: &[u8]) -> Option<i64> {
    unsafe { parse_ts(b.as_ptr(), b.len()) }
}

// ---------------------------------------------------------------------------
// Fast paths.

const T0: i64 = 1970;
const TYEARS: usize = 130; // 1970..2100

fn day_table() -> &'static [i32] {
    static T: std::sync::OnceLock<Vec<i32>> = std::sync::OnceLock::new();
    T.get_or_init(|| {
        let mut v = vec![-1i32; TYEARS * 12 * 32];
        for y in 0..TYEARS {
            for m in 1..=12 {
                let yy = T0 + y as i64;
                for d in 1..=days_in_month(yy, m) {
                    v[(y * 12 + (m as usize - 1)) * 32 + d as usize] = days_from_civil(yy, m, d) as i32;
                }
            }
        }
        v
    })
}

pub fn init() {
    let _ = day_table();
}

#[inline(always)]
fn digit_bad(x: u64, positions: u64) -> u64 {
    // per byte: bad unless 0x30..=0x39 (no carries across bytes)
    let t = x ^ 0x3030_3030_3030_3030;
    let t7 = t & 0x7F7F_7F7F_7F7F_7F7F;
    (t | t7.wrapping_add(0x7676_7676_7676_7676)) & 0x8080_8080_8080_8080 & positions
}

/// F1 `YYYY-MM-DDTHH:MM:SS` + `Z` (20 bytes at `p`) to seconds; years outside
/// the table fall back to the general parser.
#[inline(always)]
pub unsafe fn f1_secs(p: *const u8) -> Option<i64> {
    let a = (p as *const u64).read_unaligned();
    let b = (p.add(8) as *const u64).read_unaligned();
    let c = (p.add(16) as *const u32).read_unaligned() as u64;
    let sep = ((a & 0xFF00_00FF_0000_0000) ^ ((b'-' as u64) << 56 | (b'-' as u64) << 32))
        | ((b & 0x0000_FF00_00FF_0000) ^ ((b':' as u64) << 40 | (b'T' as u64) << 16))
        | ((c & 0xFF00_00FF) ^ ((b'Z' as u64) << 24 | b':' as u64));
    // digits: a bytes 0,1,2,3,5,6; b bytes 0,1,3,4,6,7; c bytes 1,2
    let bad = digit_bad(a, 0x0080_8000_8080_8080) | digit_bad(b, 0x8080_0080_8000_8080) | digit_bad(c, 0x0000_0000_0080_8000);
    if (sep | bad) != 0 {
        return None;
    }
    let d = |x: u64, k: u32| ((x >> (8 * k)) & 0xF) as i64;
    let year = d(a, 0) * 1000 + d(a, 1) * 100 + d(a, 2) * 10 + d(a, 3);
    let month = d(a, 5) * 10 + d(a, 6);
    let day = d(b, 0) * 10 + d(b, 1);
    let hour = d(b, 3) * 10 + d(b, 4);
    let minute = d(b, 6) * 10 + d(b, 7);
    let second = d(c, 1) * 10 + d(c, 2);
    if hour > 23 || minute > 59 || second > 59 || month < 1 || month > 12 || day > 31 {
        return None;
    }
    let yi = year - T0;
    let days = if (0..TYEARS as i64).contains(&yi) {
        let v = *day_table().get_unchecked(((yi as usize * 12) + month as usize - 1) * 32 + day as usize);
        if v < 0 {
            return None;
        }
        v as i64
    } else {
        if day < 1 || day > days_in_month(year, month) {
            return None;
        }
        days_from_civil(year, month, day)
    };
    Some(days * 86_400 + hour * 3600 + minute * 60 + second)
}

/// Value of `len` (1..=16) ASCII digits ending at `e` (exclusive); `e - 16`
/// must be readable. None if any byte is not a digit.
#[inline(always)]
unsafe fn digits16(e: *const u8, len: usize) -> Option<i64> {
    let lo = (e.sub(16) as *const u64).read_unaligned();
    let hi = (e.sub(8) as *const u64).read_unaligned();
    // keep the last `len` bytes; the rest become '0'
    let keep_hi: u64 = if len >= 8 { !0 } else { !((1u64 << (8 * (8 - len))) - 1) };
    let keep_lo: u64 = if len >= 16 { !0 } else if len <= 8 { 0 } else { !((1u64 << (8 * (16 - len))) - 1) };
    let hi = (hi & keep_hi) | (0x3030_3030_3030_3030 & !keep_hi);
    let lo = (lo & keep_lo) | (0x3030_3030_3030_3030 & !keep_lo);
    if (digit_bad(hi, 0x8080_8080_8080_8080) | digit_bad(lo, 0x8080_8080_8080_8080)) != 0 {
        return None;
    }
    #[inline(always)]
    fn parse8(v: u64) -> u64 {
        let v = v.wrapping_sub(0x3030_3030_3030_3030);
        let v = v.wrapping_mul(10).wrapping_add(v >> 8);
        (((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32)))
            .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
            >> 32
    }
    Some((parse8(lo).wrapping_mul(100_000_000).wrapping_add(parse8(hi))) as i64)
}

/// Like `parse_ts`, with fast paths. `p - 16` must be readable when `len <= 16`.
#[inline(always)]
pub unsafe fn parse_ts_fast(p: *const u8, len: usize) -> Option<i64> {
    if len == 20 {
        if *p.add(19) == b'Z' {
            return f1_secs(p).map(|s| s * 1000);
        }
        return parse_ts(p, len);
    }
    if len >= 1 && len <= 16 {
        return digits16(p.add(len), len);
    }
    parse_ts(p, len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast(s: &[u8]) -> Option<i64> {
        // 16 bytes of junk before, like a real line
        let mut buf = b"xxxxxxxxxxxxxxxx".to_vec();
        buf.extend_from_slice(s);
        buf.extend_from_slice(b",yyyyyyyyyyyyyyyyyyyyyyyy");
        if s.len() <= 20 {
            unsafe { parse_ts_fast(buf.as_ptr().add(16), s.len()) }
        } else {
            parse_ts_slice(s)
        }
    }

    #[test]
    fn fuzz_against_slow() {
        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet = b"0123456789-:TZtz+. \x00\xff\xd9";
        let mut n = 0;
        for i in 0..3_000_000u64 {
            let mut s: Vec<u8>;
            match i % 4 {
                0 => {
                    // valid-ish F1 with random mutations
                    let ms = (rnd() % 8_000_000_000_000) as i64 - 1_000_000_000_000;
                    let days = ms.div_euclid(86_400_000);
                    let _ = days;
                    let y = 1900 + rnd() % 260;
                    s = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, rnd() % 14, rnd() % 33, rnd() % 26, rnd() % 62, rnd() % 62).into_bytes();
                    if rnd() % 3 == 0 {
                        let k = (rnd() % s.len() as u64) as usize;
                        s[k] = alphabet[(rnd() % alphabet.len() as u64) as usize];
                    }
                }
                1 => {
                    let len = 1 + rnd() % 20;
                    s = (0..len).map(|_| b"0123456789"[(rnd() % 10) as usize]).collect();
                    if rnd() % 4 == 0 {
                        let k = (rnd() % s.len() as u64) as usize;
                        s[k] = alphabet[(rnd() % alphabet.len() as u64) as usize];
                    }
                }
                2 => {
                    let y = 1960 + rnd() % 150;
                    let sign = if rnd() % 2 == 0 { '+' } else { '-' };
                    s = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}{:02}:{:02}", y, 1 + rnd() % 12, 1 + rnd() % 31, rnd() % 25, rnd() % 61, rnd() % 61, sign, rnd() % 26, rnd() % 62).into_bytes();
                }
                _ => {
                    let len = rnd() % 26;
                    s = (0..len).map(|_| alphabet[(rnd() % alphabet.len() as u64) as usize]).collect();
                }
            }
            let a = parse_ts_slice(&s);
            let b = fast(&s);
            assert_eq!(a, b, "{:?}", String::from_utf8_lossy(&s));
            n += a.is_some() as u64;
        }
        assert!(n > 100_000, "{n}");
        // a few exact ones
        assert_eq!(fast(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        assert_eq!(fast(b"1791071890000"), Some(1791071890000));
        assert_eq!(fast(b"0001791071890000"), Some(1791071890000));
        assert_eq!(fast(b"2028-02-29T00:00:00Z").is_some(), true);
        assert_eq!(fast(b"2027-02-29T00:00:00Z"), None);
    }
}
