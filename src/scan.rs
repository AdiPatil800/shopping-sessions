//! Structural scanning: 64 bytes at a time, bitmasks of `\n`, `,` and `"`.

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let cm = _mm256_set1_epi8(b',' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    let m = |x: __m256i, c: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, c)) as u32 as u64;
    (m(a, nl) | (m(b, nl) << 32), m(a, cm) | (m(b, cm) << 32), m(a, qt) | (m(b, qt) << 32))
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    use std::arch::aarch64::*;
    // movemask emulation: AND with bit weights, then pairwise adds.
    let w: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
    let wv = vld1q_u8(w.as_ptr());
    let mm = |v: uint8x16_t| -> u64 {
        let x = vandq_u8(v, wv);
        let lo = vaddv_u8(vget_low_u8(x)) as u64;
        let hi = vaddv_u8(vget_high_u8(x)) as u64;
        lo | (hi << 8)
    };
    let mut nl = 0u64;
    let mut cm = 0u64;
    let mut qt = 0u64;
    for k in 0..4 {
        let v = vld1q_u8(p.add(16 * k));
        nl |= mm(vceqq_u8(v, vdupq_n_u8(b'\n'))) << (16 * k);
        cm |= mm(vceqq_u8(v, vdupq_n_u8(b','))) << (16 * k);
        qt |= mm(vceqq_u8(v, vdupq_n_u8(b'"'))) << (16 * k);
    }
    (nl, cm, qt)
}

#[cfg(not(any(all(target_arch = "x86_64", target_feature = "avx2"), all(target_arch = "aarch64", target_feature = "neon"))))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    let (mut nl, mut cm, mut qt) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let c = *p.add(i);
        nl |= ((c == b'\n') as u64) << i;
        cm |= ((c == b',') as u64) << i;
        qt |= ((c == b'"') as u64) << i;
    }
    (nl, cm, qt)
}

/// Calls `f(line_start, line_end, commas, n_commas, n_quotes)` for every line
/// in `buf[start..end]` (`end` is a line boundary or the end of `buf`).
/// `commas` holds the first 8 comma positions of the line.
#[inline(always)]
pub fn scan_lines(buf: &[u8], start: usize, end: usize, mut f: impl FnMut(usize, usize, &[usize; 8], usize, u32)) {
    let len = buf.len();
    let base = buf.as_ptr();
    let mut commas = [0usize; 8];
    let mut nc = 0usize;
    let mut qc = 0u32;
    let mut ls = start;
    let mut p = start;
    let mut tail = [0u8; 64];
    while p < end {
        let (nl, cm, qm) = unsafe {
            if p + 64 <= len {
                masks(base.add(p))
            } else {
                tail = [0u8; 64];
                tail[..len - p].copy_from_slice(&buf[p..]);
                masks(tail.as_ptr())
            }
        };
        let valid = if end - p >= 64 { !0u64 } else { (1u64 << (end - p)) - 1 };
        let cm = cm & valid;
        let mut qrem = qm & valid;
        let mut seps = (nl | cm) & valid;
        while seps != 0 {
            let i = seps.trailing_zeros() as usize;
            let bit = 1u64 << i;
            seps &= seps - 1;
            if cm & bit != 0 {
                if nc < 8 {
                    commas[nc] = p + i;
                }
                nc += 1;
            } else {
                let below = bit - 1;
                qc += (qrem & below).count_ones();
                qrem &= !below;
                f(ls, p + i, &commas, nc, qc);
                ls = p + i + 1;
                nc = 0;
                qc = 0;
            }
        }
        qc += qrem.count_ones();
        p += 64;
    }
    if ls < end {
        f(ls, end, &commas, nc, qc);
    }
}

/// Field boundaries of a line with exactly N-1 commas, quotes removed.
/// Returns false if the quoting is malformed.
#[inline(always)]
pub fn fields<const N: usize>(buf: &[u8], ls: usize, le: usize, commas: &[usize; 8], qc: u32, out: &mut [(usize, usize); N]) -> bool {
    out[0].0 = ls;
    for k in 0..N - 1 {
        out[k].1 = commas[k];
        out[k + 1].0 = commas[k] + 1;
    }
    out[N - 1].1 = le;
    if qc == 0 {
        return true;
    }
    let mut nq = 0u32;
    for f in out.iter_mut() {
        let (s, e) = *f;
        if s < e && buf[s] == b'"' {
            if e - s < 2 || buf[e - 1] != b'"' {
                return false;
            }
            *f = (s + 1, e - 1);
            nq += 1;
        }
    }
    qc == 2 * nq
}
