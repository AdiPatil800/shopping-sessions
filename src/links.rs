//! Linked IDs, in parallel: a concurrent hash table of 4-slot buckets (one
//! cache line each) whose slot index is the node id, and a lock-free union-find over slots. A root is only
//! ever linked under a root with a byte-wise smaller ID, so there are no
//! cycles and every set's root is its smallest ID: the person's name.

use crate::ids::{cmp_ids, SLOW_BIT};
use crate::sys;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};

const K2: u64 = 0xD6E8_FEB8_6659_FD93;

pub struct PersonTable {
    slots: &'static [[AtomicU64; 2]], // (key, person name); key 0 = empty
    parent: &'static [AtomicU32],     // 0 = root, else parent + 1
    strs: &'static [AtomicU64],       // where a hashed key's bytes are (0 for packed keys)
    shift: u32,
    mask: usize,
}

impl PersonTable {
    fn new(expected_keys: usize) -> Self {
        let cap = (expected_keys * 2).next_power_of_two().max(1024);
        debug_assert!(cap % 4 == 0);
        let slots = unsafe { std::slice::from_raw_parts(sys::anon(cap * 16, true) as *const [AtomicU64; 2], cap) };
        let parent = unsafe { std::slice::from_raw_parts(sys::anon(cap * 4, true) as *const AtomicU32, cap) };
        let strs = unsafe { std::slice::from_raw_parts(sys::anon(cap * 8, true) as *const AtomicU64, cap) };
        PersonTable { slots, parent, strs, shift: 64 - (cap / 4).trailing_zeros(), mask: cap - 1 }
    }

    /// First slot of the key's bucket (4 slots, one cache line).
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        ((k.wrapping_mul(K2) >> self.shift) as usize) << 2
    }

    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(self.slots.as_ptr().add(self.slot(k)) as *const i8, std::arch::x86_64::_MM_HINT_T0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) self.slots.as_ptr().add(self.slot(k)), options(nostack, preserves_flags));
        }
    }

    #[inline(always)]
    fn prefetch_parent(&self, i: usize) {
        let p = unsafe { self.parent.as_ptr().add(i) };
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, preserves_flags));
        }
    }

    /// The person name of an ID (the ID itself when it is in no link). Only
    /// for use once the table is complete. Slots of a bucket fill in order, so
    /// an empty last slot means the key is absent; the common cases take no
    /// data-dependent branch.
    #[inline(always)]
    pub fn person(&self, k: u64) -> u64 {
        let mut i = self.slot(k);
        loop {
            // names are never 0, so a zero value means "not in this bucket"
            let (_, v, full) = unsafe { probe_bucket(self.slots.as_ptr().add(i) as *const u64, k) };
            let missing = (v == 0) as u64;
            if (missing & full as u64) == 0 {
                return v | (k & missing.wrapping_neg());
            }
            i = (i + 4) & self.mask;
        }
    }

    /// Returns (slot, newly inserted by this call). `loc` says where the ID's
    /// bytes are; every caller records it for a hashed key before using the
    /// slot, so it is set by the time anyone compares through this slot.
    #[inline]
    fn insert(&self, k: u64, loc: u64) -> (usize, bool) {
        let mut i = self.slot(k);
        loop {
            for j in i..i + 4 {
                let s = &self.slots[j][0];
                let x = s.load(Relaxed);
                let (hit, new) = if x == k {
                    (true, false)
                } else if x == 0 {
                    match s.compare_exchange(0, k, Relaxed, Relaxed) {
                        Ok(_) => (true, true),
                        Err(y) => (y == k, false),
                    }
                } else {
                    (false, false)
                };
                if hit {
                    if k & SLOW_BIT != 0 && self.strs[j].load(Relaxed) == 0 {
                        self.strs[j].store(loc, Relaxed);
                    }
                    return (j, new);
                }
            }
            i = (i + 4) & self.mask;
        }
    }

    /// Where the bytes of a hashed key in the table are, if it is in the table.
    pub fn name_loc(&self, k: u64) -> Option<u64> {
        let mut i = self.slot(k);
        loop {
            for j in i..i + 4 {
                let x = self.slots[j][0].load(Relaxed);
                if x == k {
                    return Some(self.strs[j].load(Relaxed));
                }
                if x == 0 {
                    return None;
                }
            }
            i = (i + 4) & self.mask;
        }
    }

    #[inline]
    fn find(&self, mut x: usize) -> usize {
        loop {
            let p = self.parent[x].load(Relaxed);
            if p == 0 {
                return x;
            }
            let p = (p - 1) as usize;
            let gp = self.parent[p].load(Relaxed);
            if gp == 0 {
                return p;
            }
            self.parent[x].store(gp, Relaxed);
            x = (gp - 1) as usize;
        }
    }

    #[inline]
    fn union(&self, a: usize, b: usize) {
        loop {
            let (ra, rb) = (self.find(a), self.find(b));
            if ra == rb {
                return;
            }
            let (ka, kb) = (self.slots[ra][0].load(Relaxed), self.slots[rb][0].load(Relaxed));
            let (la, lb) = (self.strs[ra].load(Relaxed), self.strs[rb].load(Relaxed));
            let (lo, hi) = if cmp_ids(ka, la, kb, lb) == Ordering::Less { (ra, rb) } else { (rb, ra) };
            if self.parent[hi].compare_exchange(0, lo as u32 + 1, Relaxed, Relaxed).is_ok() {
                return;
            }
        }
    }

    /// Builds the table from link pairs (one Vec per producer), with `n` threads.
    pub fn build(pairs: &[Vec<(u64, u64, u64, u64)>], n: usize) -> PersonTable {
        let total: usize = pairs.iter().map(|v| v.len()).sum();
        let t0 = std::time::Instant::now();
        let t = PersonTable::new(total * 3 / 2 + 16);
        let prof = std::env::var("PROF").is_ok();
        const JOB: usize = 1 << 14;
        let jobs: Vec<(usize, usize)> = pairs.iter().enumerate().flat_map(|(v, p)| (0..p.len()).step_by(JOB).map(move |s| (v, s))).collect();
        let next = AtomicUsize::new(0);
        let inserted: Vec<Vec<u32>> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..n)
                .map(|_| {
                    s.spawn(|| {
                        let mut mine: Vec<u32> = Vec::new();
                        let mut ids: Vec<(u32, u32)> = Vec::with_capacity(JOB);
                        loop {
                            let j = next.fetch_add(1, Relaxed);
                            let Some(&(v, st)) = jobs.get(j) else { break };
                            let sl = &pairs[v][st..(st + JOB).min(pairs[v].len())];
                            // 1. insert both ends (table misses hidden by prefetch)
                            ids.clear();
                            for (i, &(a, la, b, lb)) in sl.iter().enumerate() {
                                if let Some(&(c, _, d, _)) = sl.get(i + 8) {
                                    t.prefetch(c);
                                    t.prefetch(d);
                                }
                                let (ia, na) = t.insert(a, la);
                                let (ib, nb) = t.insert(b, lb);
                                if na {
                                    mine.push(ia as u32);
                                }
                                if nb {
                                    mine.push(ib as u32);
                                }
                                ids.push((ia as u32, ib as u32));
                            }
                            // 2. union (parent entries prefetched)
                            for (i, &(ia, ib)) in ids.iter().enumerate() {
                                if let Some(&(c, d)) = ids.get(i + 8) {
                                    t.prefetch_parent(c as usize);
                                    t.prefetch_parent(d as usize);
                                }
                                if ia != ib {
                                    t.union(ia as usize, ib as usize);
                                }
                            }
                        }
                        mine
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        if prof {
            eprintln!("  links: insert+union done at {:.1} ms ({} ids)", t0.elapsed().as_secs_f64() * 1e3, inserted.iter().map(|v| v.len()).sum::<usize>());
        }
        // every slot learns its root's key
        std::thread::scope(|s| {
            for mine in &inserted {
                let t = &t;
                s.spawn(move || {
                    for (k, &i) in mine.iter().enumerate() {
                        if let Some(&j) = mine.get(k + 8) {
                            t.prefetch_parent(j as usize);
                        }
                        let r = t.find(i as usize);
                        let name = t.slots[r][0].load(Relaxed);
                        t.slots[i as usize][1].store(name, Relaxed);
                    }
                });
            }
        });
        if prof {
            eprintln!("  links: names done at {:.1} ms", t0.elapsed().as_secs_f64() * 1e3);
        }
        t
    }
}

/// Looks for `k` among the four (key, value) pairs of a bucket: (found, value,
/// bucket is full). No data-dependent branches.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn probe_bucket(b: *const u64, k: u64) -> (bool, u64, bool) {
    use std::arch::x86_64::*;
    let kk = _mm256_set1_epi64x(k as i64);
    let even = _mm256_setr_epi64x(-1, 0, -1, 0);
    let x = _mm256_loadu_si256(b as *const __m256i);
    let y = _mm256_loadu_si256(b.add(4) as *const __m256i);
    let mx = _mm256_and_si256(_mm256_cmpeq_epi64(x, kk), even);
    let my = _mm256_and_si256(_mm256_cmpeq_epi64(y, kk), even);
    // move each key's mask onto its value lane, keep the matching value
    let vx = _mm256_and_si256(x, _mm256_slli_si256(mx, 8));
    let vy = _mm256_and_si256(y, _mm256_slli_si256(my, 8));
    let v = _mm256_or_si256(vx, vy);
    let v128 = _mm_or_si128(_mm256_castsi256_si128(v), _mm256_extracti128_si256(v, 1));
    let value = _mm_extract_epi64(v128, 1) as u64;
    let found = _mm256_testz_si256(_mm256_or_si256(mx, my), _mm256_or_si256(mx, my)) == 0;
    (found, value, *b.add(6) != 0)
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
unsafe fn probe_bucket(b: *const u64, k: u64) -> (bool, u64, bool) {
    let b = &*(b as *const [u64; 8]);
    let (m0, m1, m2, m3) = ((b[0] == k) as u64, (b[2] == k) as u64, (b[4] == k) as u64, (b[6] == k) as u64);
    let v = (b[1] & m0.wrapping_neg()) | (b[3] & m1.wrapping_neg()) | (b[5] & m2.wrapping_neg()) | (b[7] & m3.wrapping_neg());
    ((m0 | m1 | m2 | m3) != 0, v, b[6] != 0)
}
