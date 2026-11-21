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
