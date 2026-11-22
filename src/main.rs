//! Shopping Sessions pipeline: one static binary, no crates.
//!
//! Phases:
//!  1. lookups (manifest, products, campaigns), links -> union-find -> person names
//!  2. parse events and clicks in parallel (SIMD line/field scanning). Every
//!     per-copy check is done here. A valid event copy goes to its person's
//!     partition (ER); every event copy, valid or not, also leaves a small dedup
//!     record (SR: event key, ingest, where its ER lives) in a bucket by event key.
//!     Valid clicks go to their person's partition (CR).
//!  3. per event-key bucket: the copy with the smallest ingest_ts wins; every
//!     other copy's ER is marked dead (and all of them if the winner is invalid).
//!  4. per person partition: group by person (counting sort), sort by ts,
//!     sessionize, credit and assist, aggregate.
//!  5. write the two Parquet tables.

mod buf;
mod ids;
mod links;
mod parquet;
mod scan;
mod scan2;
mod sys;
mod ts;

use buf::{Arena, Streams};
use ids::{cmp_key, id_key, id_key_slice, key_bytes};
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

#[global_allocator]
static ALLOC: sys::Bump = sys::Bump;

const K1: u64 = 0x9E37_79B9_7F4A_7C15;
const K2: u64 = 0xD6E8_FEB8_6659_FD93;
const DAY: i64 = 86_400_000;
const GAP: u32 = 1_800_000;
const WEEK: i64 = 7 * DAY;

const NONE: u32 = u32::MAX;
const DEAD: u32 = u32::MAX;
const BLOCK: usize = 256;

#[inline(always)]
fn top_bits(k: u64, bits: u32) -> usize {
    (k.wrapping_mul(K1) >> (64 - bits)) as usize
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, preserves_flags));
    }
}

// ---------------------------------------------------------------------------
// Open-addressing map u64 -> u64 with interleaved slots (key 0 = empty).

struct U64Map {
    slots: Vec<(u64, u64)>,
    shift: u32,
    mask: usize,
}

impl U64Map {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { slots: vec![(0, 0); cap], shift: 64 - cap.trailing_zeros(), mask: cap - 1 }
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (k.wrapping_mul(K2) >> self.shift) as usize
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        prefetch(unsafe { self.slots.as_ptr().add(self.slot(k)) });
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u64> {
        let mut i = self.slot(k);
        loop {
            let (x, v) = unsafe { *self.slots.get_unchecked(i) };
            if x == k {
                return Some(v);
            }
            if x == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Inserts `k -> v` unless present; returns the stored value.
    fn insert(&mut self, k: u64, v: u64) -> u64 {
        let mut i = self.slot(k);
        loop {
            let (x, y) = self.slots[i];
            if x == k {
                return y;
            }
            if x == 0 {
                self.slots[i] = (k, v);
                return v;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------------------
// Small helpers.

#[inline(always)]
fn find_nl(b: &[u8], mut i: usize) -> usize {
    let n = b.len();
    let p = b.as_ptr();
    unsafe {
        while i + 8 <= n {
            let w = (p.add(i) as *const u64).read_unaligned();
            let x = w ^ 0x0A0A_0A0A_0A0A_0A0A;
            let t = x.wrapping_sub(0x0101_0101_0101_0101) & !x & 0x8080_8080_8080_8080;
            if t != 0 {
                return i + (t.trailing_zeros() / 8) as usize;
            }
            i += 8;
        }
        while i < n {
            if *p.add(i) == b'\n' {
                return i;
            }
            i += 1;
        }
    }
    n
}

/// Key of a non-empty field `buf[s..e]`.
#[inline(always)]
fn key_at(buf: &[u8], s: usize, e: usize) -> u64 {
    let len = e - s;
    if len <= 9 && s + 8 <= buf.len() {
        unsafe { id_key(buf.as_ptr().add(s), len) }
    } else {
        id_key_slice(&buf[s..e])
    }
}

/// A timestamp field `b[s..e]` to milliseconds.
#[inline(always)]
fn ts_at(b: &[u8], s: usize, e: usize, dc: &mut ts::DateCache) -> Option<i64> {
    let len = e - s;
    if e >= 16 && len <= 25 && s + 25 <= b.len() {
        unsafe { ts::parse_ts_cached(b.as_ptr().add(s), len, dc) }
    } else {
        ts::parse_ts_slice(&b[s..e])
    }
}

#[inline(always)]
fn event_kind(b: &[u8], s: usize, e: usize) -> u8 {
    let len = e - s;
    if len == 4 {
        let w = u32::from_le_bytes(b[s..s + 4].try_into().unwrap());
        if w == u32::from_le_bytes(*b"view") {
            1
        } else if w == u32::from_le_bytes(*b"cart") {
            2
        } else {
            0
        }
    } else if len == 8 && u64::from_le_bytes(b[s..s + 8].try_into().unwrap()) == u64::from_le_bytes(*b"purchase") {
        3
    } else {
        0
    }
}

#[inline(always)]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = (v * 10 + d as u64).min(100_000_000);
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

/// Rows of a small CSV file after its header (malformed lines dropped).
fn small_csv<const N: usize>(path: &Path) -> Vec<[Vec<u8>; N]> {
    let bytes = std::fs::read(path).unwrap_or_default();
    let mut rows = Vec::new();
    let Some(h) = bytes.iter().position(|&c| c == b'\n') else { return rows };
    let mut f = [(0usize, 0usize); N];
    scan::scan_lines(&bytes, h + 1, bytes.len(), |ls, le, commas, nc, qc| {
        if nc == N - 1 && scan::fields::<N>(&bytes, ls, le, commas, qc, &mut f) {
            rows.push(std::array::from_fn(|i| bytes[f[i].0..f[i].1].to_vec()));
        }
    });
    rows
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .map(|e| e.path())
                .filter(|p| {
                    let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    name.ends_with(".csv") && !name.starts_with('.') && p.is_file()
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

#[derive(Clone, Copy)]
struct Chunk {
    file: u32,
    kind: u8, // 0 events, 1 clicks, 2 links
    start: usize,
    end: usize,
}

fn make_chunks(maps: &[sys::Mapped], first: usize, kind: u8, size: usize, out: &mut Vec<Chunk>) {
    for (i, m) in maps.iter().enumerate() {
        let b = m.bytes();
        let mut pos = match b.iter().position(|&c| c == b'\n') {
            Some(i) => i + 1,
            None => continue,
        };
        while pos < b.len() {
            let end = if pos + size >= b.len() { b.len() } else { (find_nl(b, pos + size) + 1).min(b.len()) };
            out.push(Chunk { file: (first + i) as u32, kind, start: pos, end });
            pos = end;
        }
    }
}

fn run_threads<R: Send>(n: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..n).map(|t| {
            let f = &f;
            s.spawn(move || f(t))
        }).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------------------
// Records.

/// A parsed event copy, chunk-local.
#[derive(Clone, Copy)]
#[repr(C)]
struct LR {
    ek: u64,
    uk: u64,
    ingest: u32,
    ts: u32,
    pay: u32,
    valid: u32,
}

/// A valid event copy in its person partition. `pay` is 0 for a view or cart,
/// price | category << 24 for a purchase, DEAD once dedup has dropped it.
#[derive(Clone, Copy)]
#[repr(C)]
struct ER {
    person: u64,
    ts: u32,
    pay: u32,
}

/// Dedup record: event key, ingest (seconds, offset), index of its ER in the
/// thread's arena (NONE for an invalid copy).
#[derive(Clone, Copy)]
#[repr(C)]
struct SR {
    ek: u64,
    ingest: u32,
    loc: u32,
}

/// A valid click in its person partition.
#[derive(Clone, Copy)]
#[repr(C)]
struct CR {
    person: u64,
    ck: u64,
    ts: u32,
    camp: u32,
}

/// A parsed click, chunk-local.
#[derive(Clone, Copy)]
struct LC {
    uk: u64,
    ck: u64,
    ts: u32,
    camp: u32,
}

struct Lookups {
    prod: U64Map,
    cat_names: Vec<Vec<u8>>,
    unknown_cat: u32,
    camp: U64Map,
    camp_ids: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
}

struct Ctx {
    base: i64, // window_start - 7 days; every ts is stored relative to it
    base_s: i64,
    ws_rel: u32,
    we_rel: u32,
    day0: i64,     // epoch day of window_start
    day_end0: i64, // relative ts at which day0 ends
    nd: usize,     // days a session can start on
    nbins: usize,
    lk: Lookups,
    ncats: usize,
    ncamp: usize,
    person: links::PersonTable,
    pbits: u32,
    bbits: u32,
}

struct Parsed {
    es: Streams<ER>,
    ss: Streams<SR>,
    cs: Streams<CR>,
    arena: Arena,
}

fn prof() -> bool {
    static P: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *P.get_or_init(|| std::env::var("PROF").is_ok())
}

fn log(t0: &Instant, what: &str) {
    eprintln!("[{:8.3} ms] {what}", t0.elapsed().as_secs_f64() * 1e3);
    #[cfg(feature = "selfbench")]
    {
        let w: String = what.chars().take(9).collect();
        PHASES.lock().unwrap().push_str(&format!("{w}={:.1} ", t0.elapsed().as_secs_f64() * 1e3));
    }
}

/// Bounds of field `i` of a row (quotes stripped). `rs` is the row start and
/// `q` its separators, both relative to `start`.
macro_rules! field {
    ($p:expr, $start:expr, $rs:expr, $q:expr, $i:expr) => {{
        let s = if $i == 0 { $start + $rs as usize } else { $start + $q[$i - 1] as usize + 1 };
        let e = $start + $q[$i] as usize;
        let qf = unsafe { *$p.add(s) == b'"' } as usize;
        (s + qf, e - qf)
    }};
}

/// Same for the last field, which may end at the very end of the buffer.
macro_rules! last_field {
    ($b:expr, $start:expr, $q:expr, $i:expr) => {{
        let s = $start + $q[$i - 1] as usize + 1;
        let e = $start + $q[$i] as usize;
        if s < e && $b[s] == b'"' {
            (s + 1, e - 1)
        } else {
            (s, e)
        }
    }};
}

fn load_lookups(data: &Path) -> Lookups {
    // products: (key, temporary category id), then ids become ranks by name
    let bytes = std::fs::read(data.join("products.csv")).unwrap_or_default();
    let mut names: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
    let mut by_name: std::collections::HashMap<Vec<u8>, u32> = std::collections::HashMap::new();
    by_name.insert(b"unknown".to_vec(), 0);
    let mut rows: Vec<(u64, u32)> = Vec::with_capacity(bytes.len() / 12);
    if let Some(h) = bytes.iter().position(|&c| c == b'\n') {
        let mut f = [(0usize, 0usize); 2];
        let mut last: (usize, usize, u32) = (0, 0, 0);
        scan::scan_lines(&bytes, h + 1, bytes.len(), |ls, le, commas, nc, qc| {
            if nc != 1 || !scan::fields::<2>(&bytes, ls, le, commas, qc, &mut f) || f[0].0 == f[0].1 {
                return;
            }
            let name = &bytes[f[1].0..f[1].1];
            let id = if name == &bytes[last.0..last.1] && last.1 > last.0 {
                last.2
            } else {
                match by_name.get(name) {
                    Some(&i) => i,
                    None => {
                        let i = names.len() as u32;
                        names.push(name.to_vec());
                        by_name.insert(name.to_vec(), i);
                        i
                    }
                }
            };
            last = (f[1].0, f[1].1, id);
            rows.push((key_at(&bytes, f[0].0, f[0].1), id));
        });
    }
    let mut order: Vec<u32> = (0..names.len() as u32).collect();
    order.sort_by(|&a, &b| names[a as usize].cmp(&names[b as usize]));
    let mut rank = vec![0u32; names.len()];
    for (r, &i) in order.iter().enumerate() {
        rank[i as usize] = r as u32;
    }
    let cat_names: Vec<Vec<u8>> = order.iter().map(|&i| names[i as usize].clone()).collect();
    assert!(cat_names.len() <= 255, "more than 255 categories is not supported");
    let mut prod = U64Map::new(rows.len());
    for &(k, id) in &rows {
        prod.insert(k, rank[id as usize] as u64);
    }
    let campaigns = small_csv::<2>(&data.join("campaigns.csv"));
    let mut camp = U64Map::new(campaigns.len());
    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    for r in &campaigns {
        if r[0].is_empty() {
            continue;
        }
        let idx = camp_ids.len() as u64;
        if camp.insert(id_key_slice(&r[0]), idx) == idx {
            camp_ids.push(r[0].clone());
            camp_channels.push(r[1].clone());
        }
    }
    Lookups { prod, unknown_cat: rank[0], cat_names, camp, camp_ids, camp_channels }
}

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(std::env::var("DATA_DIR").ok().or_else(|| args.get(1).cloned()).unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").ok().or_else(|| args.get(2).cloned()).unwrap_or("/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    // --- manifest
    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let mval = |key: &str| -> i64 {
        let i = manifest.find(&format!("\"{key}\"")).expect(key) + key.len() + 2;
        let rest = &manifest[i..];
        let a = rest.find('"').unwrap() + 1;
        let b = a + rest[a..].find('"').unwrap();
        ts::parse_ts_slice(rest[a..b].as_bytes()).expect("manifest timestamp")
    };
    if prof() {
        log(&t0, "manifest read");
    }
    let ws = mval("window_start");
    let we = mval("window_end");
    let base = ws - WEEK;
    assert!(we > ws && we - base < (1i64 << 32) - 1, "window too long");

    // --- map every input file
    let ev_files = csv_files(&data.join("events"));
    let ck_files = csv_files(&data.join("clicks"));
    let ln_files = csv_files(&data.join("links"));
    if prof() {
        log(&t0, "listed");
    }
    let mut maps: Vec<sys::Mapped> = Vec::new();
    let mut files: Vec<std::fs::File> = Vec::new();
    for p in ev_files.iter().chain(&ck_files).chain(&ln_files) {
        let (m, f) = sys::Mapped::open(p, false).expect("mmap");
        maps.push(m);
        files.push(f);
    }
    let (nev, nck) = (ev_files.len(), ck_files.len());
    // The helper threads start only now: their page faults would make every
    // mmap call above wait for the address-space lock.
    let lookups = {
        let data = data.clone();
        std::thread::spawn(move || {
            ts::init();
            load_lookups(&data)
        })
    };
    let dirs = {
        let out = out.clone();
        std::thread::spawn(move || {
            std::fs::create_dir_all(out.join("user_daily")).expect("mkdir user_daily");
            std::fs::create_dir_all(out.join("campaign_daily")).expect("mkdir campaign_daily");
        })
    };

    let ev_bytes: usize = maps[..nev].iter().map(|m| m.len).sum();
    let ck_bytes: usize = maps[nev..nev + nck].iter().map(|m| m.len).sum();
    log(&t0, &format!("mapped {} event files {} B, {} click files {} B, {} link files", nev, ev_bytes, nck, ck_bytes, ln_files.len()));

    dirs.join().unwrap();
    let unmap = std::env::var("KEEP_MAPS").is_err() && !cfg!(feature = "selfbench");
    let ctx = pipeline(&maps, &files, nev, nck, ws, we, move || lookups.join().unwrap(), &out, nthreads, unmap, &|w: &str| log(&t0, w));
    #[cfg(feature = "selfbench")]
    {
        let rows = self_bench(&ctx, &maps, &files, nev, nck, nthreads, ws, we, &data, &out);
        write_extra_rows(&out.join("user_daily"), ctx.day0, &rows);
    }
    let _ = &ctx;
    sys::exit_now(0);
}

/// Everything after the inputs are mapped: `maps` holds the event files, then
/// the click files, then the link files.
#[allow(clippy::too_many_arguments)]
fn pipeline(maps: &[sys::Mapped], files: &[std::fs::File], nev: usize, nck: usize, ws: i64, we: i64, lookups: impl FnOnce() -> Lookups, out: &Path, nthreads: usize, unmap: bool, log: &dyn Fn(&str)) -> Ctx {
    let base = ws - WEEK;
    let day0 = ws.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let ev_bytes: usize = maps[..nev].iter().map(|m| m.len).sum();
    let ck_bytes: usize = maps[nev..nev + nck].iter().map(|m| m.len).sum();
    // --- links
    let mut lchunks = Vec::new();
    make_chunks(&maps[nev + nck..], nev + nck, 2, 256 << 10, &mut lchunks);
    let next = AtomicUsize::new(0);
    let pairs: Vec<Vec<(u64, u64)>> = run_threads(nthreads, |_| {
        let mut v = Vec::new();
        let mut ix = scan2::Index::new();
        loop {
            let k = next.fetch_add(1, Relaxed);
            let Some(c) = lchunks.get(k) else { break };
            let b = maps[c.file as usize].bytes();
            let (start, p) = (c.start, b.as_ptr());
            scan2::index(b, start, c.end, &mut ix);
            scan2::rows::<2>(&ix, |rs, q| {
                let (s0, e0) = field!(p, start, rs, q, 0);
                let (s1, e1) = last_field!(b, start, q, 1);
                if s0 == e0 || s1 == e1 {
                    return;
                }
                v.push((key_at(b, s0, e0), key_at(b, s1, e1)));
            });
        }
        v
    });
    let npairs: usize = pairs.iter().map(|v| v.len()).sum();
    log("links parsed");
    let person = links::PersonTable::build(&pairs, nthreads);
    log(&format!("links: {npairs} pairs"));
    let lk = lookups();
    let (ncats, ncamp) = (lk.cat_names.len(), lk.camp_ids.len());

    let est_rows = ev_bytes / 76 + 1;
    let envu = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<usize>().ok());
    let bits = (usize::BITS - (est_rows / 40_000).leading_zeros()).clamp(6, 11);
    let pbits = envu("PBITS").or(envu("BITS")).map(|v| v as u32).unwrap_or(bits);
    let bbits = envu("BBITS").or(envu("BITS")).map(|v| v as u32).unwrap_or(bits);
    let ctx = Ctx {
        base,
        base_s: base.div_euclid(1000),
        ws_rel: (ws - base) as u32,
        we_rel: (we - base) as u32,
        day0,
        day_end0: (day0 + 1) * DAY - base,
        nd,
        nbins: ((we - base) / GAP as i64) as usize + 2,
        lk,
        ncats,
        ncamp,
        person,
        pbits,
        bbits,
    };
    let (np, nb) = (1usize << ctx.pbits, 1usize << ctx.bbits);
    log(&format!("lookups joined: {} cats, {} campaigns; {} partitions", ncats, ncamp, np));

    // --- parse events and clicks
    let mut chunks = Vec::new();
    let csize = envu("CHUNK").unwrap_or(1 << 20);
    make_chunks(&maps[..nev], 0, 0, csize, &mut chunks);
    make_chunks(&maps[nev..nev + nck], nev, 1, csize, &mut chunks);
    let arena_cap = ev_bytes / 36 * 32 + ck_bytes / 20 * 24 + (2 * np + nb) * BLOCK * 24 + (16 << 20);
    let pp = PP {
        sub: envu("SUB").unwrap_or(16 << 10),
        pbits: ctx.pbits,
        bbits: ctx.bbits,
        mode: envu("MODE").unwrap_or(0) as u8,
        pread: envu("PREAD").is_some() && !files.is_empty(),
        huge: envu("NOHUGE").is_none(),
        block: envu("BLK").unwrap_or(BLOCK),
        threads: nthreads,
        pf: envu("PFD").unwrap_or(PF),
        arena_cap,
    };
    let parsed: Vec<Parsed> = parse_phase(&ctx, maps, files, &chunks, &pp);
    log("parsed");
    // Unmap the inputs now, in parallel: at exit this would be done by one thread.
    if unmap && !pp.pread {
        const STEP: usize = 16 << 20;
        let drop_only = std::env::var("MUNMAP").is_err();
        let mut jobs: Vec<(usize, usize)> = Vec::new();
        for m in maps {
            let len = (m.len + 4095) & !4095;
            let mut o = 0;
            while o < len {
                let n = STEP.min(len - o);
                jobs.push((m.ptr as usize + o, n));
                o += n;
            }
        }
        let next = AtomicUsize::new(0);
        run_threads(nthreads, |_| loop {
            let k = next.fetch_add(1, Relaxed);
            let Some(&(p, n)) = jobs.get(k) else { break };
            if drop_only {
                sys::drop_pages(p as *const u8, n);
            } else {
                sys::unmap(p as *const u8, n);
            }
        });
        log("inputs unmapped");
    }

    // --- dedup: kill every copy but the earliest-ingested one
    let next = AtomicUsize::new(0);
    let killed: Vec<usize> = run_threads(nthreads, |_| dedup_worker(&parsed, nb, &next));
    log(&format!("dedup: {} duplicate copies", killed.iter().sum::<usize>()));

    // --- sessionize per person partition, writing user_daily as we go
    let next = AtomicUsize::new(0);
    let ud_dir = out.join("user_daily");
    // biggest partitions first: a heavy one picked last would leave the other threads idle
    let mut order: Vec<(usize, u32)> = (0..np).map(|p| (parsed.iter().map(|t| t.es.len(p) + t.cs.len(p)).sum::<usize>(), p as u32)).collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let order: Vec<u32> = order.into_iter().map(|x| x.1).collect();
    let loops: usize = std::env::var("LOOP").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    for _ in 0..loops {
        let next = AtomicUsize::new(0);
        run_threads(nthreads, |t| sessionize_worker(&ctx, &parsed, &next, &order, &ud_dir, t));
    }
    let results: Vec<(Vec<[i64; 4]>, usize)> = run_threads(nthreads, |t| sessionize_worker(&ctx, &parsed, &next, &order, &ud_dir, t));
    log("sessionized");

    let mut camp_tot = vec![[0i64; 4]; (ncamp + 1) * nd];
    for (c, _) in &results {
        for (a, b) in camp_tot.iter_mut().zip(c) {
            for j in 0..4 {
                a[j] += b[j];
            }
        }
    }
    let total_ud: usize = results.iter().map(|r| r.1).sum();
    if total_ud == 0 {
        let f = std::fs::File::create(ud_dir.join("part-000.parquet")).expect("create user_daily");
        parquet::Writer::new(f, parquet::USER_DAILY).unwrap().finish().unwrap();
    }
    write_campaign_daily(&out.join("campaign_daily").join("part-000.parquet"), &ctx.lk.camp_ids, &ctx.lk.camp_channels, &camp_tot, day0, nd);
    log(&format!("wrote {total_ud} user_daily rows"));
    ctx
}

/// Knobs of the parse phase.
#[derive(Clone, Copy)]
struct PP {
    sub: usize,
    pbits: u32,
    bbits: u32,
    mode: u8, // 0 full, 1 index only, 2 parse without scatter, 3 touch only
    pread: bool,
    huge: bool,
    block: usize,
    threads: usize,
    pf: usize,
    arena_cap: usize,
}

fn parse_phase(ctx: &Ctx, maps: &[sys::Mapped], files: &[std::fs::File], chunks: &[Chunk], pp: &PP) -> Vec<Parsed> {
    use std::os::unix::fs::FileExt;
    let next = AtomicUsize::new(0);
    let (np, nb) = (1usize << pp.pbits, 1usize << pp.bbits);
    run_threads(pp.threads, |_| {
        let mut arena = Arena::new(if pp.mode == 0 { pp.arena_cap } else { 4096 }, pp.huge);
        let mut es: Streams<ER> = Streams::new(np, pp.block);
        let mut ss: Streams<SR> = Streams::new(nb, pp.block);
        let mut cs: Streams<CR> = Streams::new(np, pp.block);
        let mut lr: Vec<LR> = Vec::with_capacity(1 << 12);
        let mut lc: Vec<LC> = Vec::with_capacity(1 << 12);
        let mut ix = scan2::Index::new();
        let mut dc = ts::DateCache::new();
        let mut rbuf: Vec<u8> = Vec::new();
        let timing = prof();
        let mut tm = [0f64; 4];
        let mut sink = 0u64;
        loop {
            let k = next.fetch_add(1, Relaxed);
            let Some(c) = chunks.get(k) else { break };
            let (b, cstart, cend): (&[u8], usize, usize) = if pp.pread {
                let len = c.end - c.start;
                if rbuf.len() < len + 128 {
                    rbuf.resize(len + 128, b'\n');
                }
                files[c.file as usize].read_exact_at(&mut rbuf[64..64 + len], c.start as u64).expect("read");
                rbuf[63] = b'\n';
                (&rbuf[..64 + len], 64, 64 + len)
            } else {
                (maps[c.file as usize].bytes(), c.start, c.end)
            };
            if pp.mode == 3 {
                let mut i = cstart;
                while i < cend {
                    sink += b[i] as u64;
                    i += 64;
                }
                continue;
            }
            let events = c.kind == 0;
            let mut pos = cstart;
            while pos < cend {
                let sub_end = sub_chunk(b, pos, cend, pp.sub);
                let q0 = if timing { Some(Instant::now()) } else { None };
                if pp.mode == 1 {
                    scan2::index(b, pos, sub_end, &mut ix);
                    sink += ix.sep[0] as u64;
                } else if events {
                    lr.clear();
                    parse_events(ctx, b, pos, sub_end, &mut ix, &mut dc, &mut lr);
                    let q1 = if timing { Some(Instant::now()) } else { None };
                    if pp.mode == 0 {
                        emit_events(ctx, &lr, &mut es, &mut ss, &mut arena, pp);
                    } else {
                        sink += lr.len() as u64;
                    }
                    if timing {
                        tm[0] += (q1.unwrap() - q0.unwrap()).as_secs_f64();
                        tm[1] += q1.unwrap().elapsed().as_secs_f64();
                    }
                } else {
                    lc.clear();
                    parse_clicks(ctx, b, pos, sub_end, &mut ix, &mut dc, &mut lc);
                    let q1 = if timing { Some(Instant::now()) } else { None };
                    if pp.mode == 0 {
                        emit_clicks(ctx, &lc, &mut cs, &mut arena, pp);
                    } else {
                        sink += lc.len() as u64;
                    }
                    if timing {
                        tm[2] += (q1.unwrap() - q0.unwrap()).as_secs_f64();
                        tm[3] += q1.unwrap().elapsed().as_secs_f64();
                    }
                }
                pos = sub_end;
            }
        }
        std::hint::black_box(sink);
        if timing {
            eprintln!("  parse thread: ev scan {:.1} emit {:.1} | ck scan {:.1} emit {:.1} ms", tm[0] * 1e3, tm[1] * 1e3, tm[2] * 1e3, tm[3] * 1e3);
        }
        Parsed { es, ss, cs, arena }
    })
}

/// The end of a cache-sized piece of `b[start..end]` that ends on a line end.
#[inline(always)]
fn sub_chunk(b: &[u8], start: usize, end: usize, w: usize) -> usize {
    if end - start <= w {
        return end;
    }
    let mut e = start + w;
    while e > start && b[e - 1] != b'\n' {
        e -= 1;
    }
    if e == start {
        e = (find_nl(&b[..end], start + w) + 1).min(end);
    }
    e
}

#[inline(always)]
fn parse_events(ctx: &Ctx, b: &[u8], start: usize, end: usize, ix: &mut scan2::Index, dc: &mut ts::DateCache, lr: &mut Vec<LR>) {
    let p = b.as_ptr();
    scan2::index(b, start, end, ix);
    scan2::rows::<7>(ix, |rs, q| {
        let (s0, e0) = field!(p, start, rs, q, 0);
        if s0 == e0 {
            return;
        }
        let ek = key_at(b, s0, e0);
        let (s3, e3) = field!(p, start, rs, q, 3);
        let ingest_ms = ts_at(b, s3, e3, dc);
        let ingest = match ingest_ms {
            Some(ms) => (ms.div_euclid(1000) - ctx.base_s + (1i64 << 31)).clamp(0, u32::MAX as i64 - 1) as u32,
            None => u32::MAX,
        };
        let mut e = LR { ek, uk: 0, ingest, ts: 0, pay: 0, valid: 0 };
        'v: {
            let (s1, e1) = field!(p, start, rs, q, 1);
            let (s5, e5) = field!(p, start, rs, q, 5);
            if s1 == e1 || s5 == e5 {
                break 'v;
            }
            let (s2, e2) = field!(p, start, rs, q, 2);
            let Some(t) = ts_at(b, s2, e2, dc) else { break 'v };
            let rel = t - ctx.base;
            if rel < ctx.ws_rel as i64 || rel >= ctx.we_rel as i64 {
                break 'v;
            }
            let (s4, e4) = field!(p, start, rs, q, 4);
            let kind = event_kind(b, s4, e4);
            if kind == 0 {
                break 'v;
            }
            match ingest_ms {
                Some(ms) if ms - t <= DAY => {}
                _ => break 'v,
            }
            if kind == 3 {
                let (s6, e6) = last_field!(b, start, q, 6);
                let Some(pr) = parse_price(&b[s6..e6]) else { break 'v };
                let pk = key_at(b, s5, e5);
                let cat = ctx.lk.prod.get(pk).map(|c| c as u32).unwrap_or(ctx.lk.unknown_cat);
                e.pay = pr | (cat << 24);
            }
            e.ts = rel as u32;
            e.uk = key_at(b, s1, e1);
            e.valid = 1;
        }
        lr.push(e);
    });
}

const PF: usize = 12;

#[inline(always)]
fn emit_events(ctx: &Ctx, lr: &[LR], es: &mut Streams<ER>, ss: &mut Streams<SR>, arena: &mut Arena, pp: &PP) {
    let base = arena.base() as usize;
    for (i, e) in lr.iter().enumerate() {
        if let Some(x) = lr.get(i + pp.pf) {
            ctx.person.prefetch(x.uk);
        }
        let mut loc = NONE;
        if e.valid != 0 {
            let person = ctx.person.person(e.uk);
            let p = es.push(top_bits(person, pp.pbits), ER { person, ts: e.ts, pay: e.pay }, arena);
            loc = ((p as usize - base) / 16) as u32;
        }
        ss.push(top_bits(e.ek, pp.bbits), SR { ek: e.ek, ingest: e.ingest, loc }, arena);
    }
}

#[inline(always)]
fn parse_clicks(ctx: &Ctx, b: &[u8], start: usize, end: usize, ix: &mut scan2::Index, dc: &mut ts::DateCache, lc: &mut Vec<LC>) {
    let p = b.as_ptr();
    scan2::index(b, start, end, ix);
    scan2::rows::<4>(ix, |rs, q| {
        let (s0, e0) = field!(p, start, rs, q, 0);
        let (s1, e1) = field!(p, start, rs, q, 1);
        let (s2, e2) = field!(p, start, rs, q, 2);
        if s0 == e0 || s1 == e1 || s2 == e2 {
            return;
        }
        let (s3, e3) = last_field!(b, start, q, 3);
        let Some(t) = ts_at(b, s3, e3, dc) else { return };
        let rel = t - ctx.base;
        if rel < 0 || rel >= ctx.we_rel as i64 {
            return;
        }
        let Some(camp) = ctx.lk.camp.get(key_at(b, s2, e2)) else { return };
        lc.push(LC { uk: key_at(b, s1, e1), ck: key_at(b, s0, e0), ts: rel as u32, camp: camp as u32 });
    });
}

#[inline(always)]
fn emit_clicks(ctx: &Ctx, lc: &[LC], cs: &mut Streams<CR>, arena: &mut Arena, pp: &PP) {
    for (i, c) in lc.iter().enumerate() {
        if let Some(x) = lc.get(i + pp.pf) {
            ctx.person.prefetch(x.uk);
        }
        let person = ctx.person.person(c.uk);
        cs.push(top_bits(person, pp.pbits), CR { person, ck: c.ck, ts: c.ts, camp: c.camp }, arena);
    }
}

// ---------------------------------------------------------------------------
// Dedup.

fn dedup_worker(parsed: &[Parsed], nb: usize, next: &AtomicUsize) -> usize {
    let mut b1: Vec<u64> = Vec::new();
    let mut b2: Vec<u64> = Vec::new();
    let mut table: Vec<(u64, u32, u32, u32)> = Vec::new();
    let mut killed = 0usize;
    let kill = |t: u32, loc: u32| {
        if loc != NONE {
            unsafe { (*(parsed[t as usize].arena.base() as *mut ER).add(loc as usize)).pay = DEAD };
        }
    };
    loop {
        let bkt = next.fetch_add(1, Relaxed);
        if bkt >= nb {
            break;
        }
        let n: usize = parsed.iter().map(|t| t.ss.len(bkt)).sum();
        if n == 0 {
            continue;
        }
        let mbits = (usize::BITS - (n * 16).leading_zeros()).max(10);
        let words = 1usize << (mbits - 6);
        b1.clear();
        b1.resize(words, 0);
        b2.clear();
        b2.resize(words, 0);
        let sh = 64 - mbits;
        let mut suspects = 0usize;
        for th in parsed {
            th.ss.for_slices(bkt, |sl| {
                for r in sl {
                    let i = (r.ek.wrapping_mul(K2) >> sh) as usize;
                    let (w, m) = (i >> 6, 1u64 << (i & 63));
                    unsafe {
                        let x = *b1.get_unchecked(w);
                        if x & m != 0 {
                            *b2.get_unchecked_mut(w) |= m;
                            suspects += 1;
                        } else {
                            *b1.get_unchecked_mut(w) = x | m;
                        }
                    }
                }
            });
        }
        if suspects == 0 {
            continue;
        }
        let cap = (suspects * 4).next_power_of_two().max(16);
        let tshift = 64 - cap.trailing_zeros();
        let mask = cap - 1;
        table.clear();
        table.resize(cap, (0, 0, 0, 0));
        for (t, th) in parsed.iter().enumerate() {
            th.ss.for_slices(bkt, |sl| {
                for r in sl {
                    let i = (r.ek.wrapping_mul(K2) >> sh) as usize;
                    if unsafe { *b2.get_unchecked(i >> 6) } & (1u64 << (i & 63)) == 0 {
                        continue;
                    }
                    let mut j = ((r.ek.wrapping_mul(K2) << mbits) >> tshift) as usize;
                    loop {
                        let slot = unsafe { table.get_unchecked_mut(j) };
                        if slot.0 == 0 {
                            *slot = (r.ek, r.ingest, r.loc, t as u32);
                            break;
                        }
                        if slot.0 == r.ek {
                            killed += 1;
                            if r.ingest < slot.1 {
                                kill(slot.3, slot.2);
                                *slot = (r.ek, r.ingest, r.loc, t as u32);
                            } else {
                                kill(t as u32, r.loc);
                            }
                            break;
                        }
                        j = (j + 1) & mask;
                    }
                }
            });
        }
    }
    killed
}

// ---------------------------------------------------------------------------
// Sessions.

#[derive(Clone, Copy)]
struct Sess {
    start: u32,
    events: u32,
    rev: i64,
    di: u32,
    purchases: u32,
}

struct UdWriter {
    path: PathBuf,
    w: Option<parquet::Writer<std::fs::File>>,
    uid: Vec<u8>,
    day: Vec<i32>,
    ses: Vec<i32>,
    evs: Vec<i32>,
    rev: Vec<i64>,
    top: Vec<u8>,
    defs: Vec<u8>,
    n: usize,
    total: usize,
}

const GROUP_ROWS: usize = 1 << 16;

fn as_bytes<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

/// The bytes of a packed (non-interned) key, first byte lowest, and their count.
#[inline(always)]
fn unpack_key(key: u64) -> (u64, u8, usize) {
    let len = 9 - (key.trailing_zeros() / 7).min(8) as usize;
    let mut x = key >> 7;
    x = ((x & 0x00FF_FFFF_F000_0000) << 4) | (x & 0x0000_0000_0FFF_FFFF);
    x = ((x & 0x0FFF_C000_0FFF_C000) << 2) | (x & 0x0000_3FFF_0000_3FFF);
    x = ((x & 0x3F80_3F80_3F80_3F80) << 1) | (x & 0x007F_007F_007F_007F);
    (x.swap_bytes(), (key & 0x7F) as u8, len)
}

impl UdWriter {
    fn new(path: PathBuf) -> Self {
        UdWriter {
            path,
            w: None,
            uid: Vec::with_capacity(GROUP_ROWS * 16 + 64),
            day: vec![0; GROUP_ROWS],
            ses: vec![0; GROUP_ROWS],
            evs: vec![0; GROUP_ROWS],
            rev: vec![0; GROUP_ROWS],
            top: Vec::with_capacity(GROUP_ROWS * 4),
            defs: vec![0; GROUP_ROWS / 8 + 8],
            n: 0,
            total: 0,
        }
    }
    #[inline(always)]
    fn push(&mut self, person: u64, day: i32, sessions: u32, events: u32, revenue: i64, top: Option<&[u8]>) {
        if person & ids::SLOW_BIT == 0 && self.uid.capacity() - self.uid.len() >= 16 {
            let (lo, ninth, len) = unpack_key(person);
            unsafe {
                let p = self.uid.as_mut_ptr().add(self.uid.len());
                (p as *mut u32).write_unaligned(len as u32);
                (p.add(4) as *mut u64).write_unaligned(lo);
                *p.add(12) = ninth;
                self.uid.set_len(self.uid.len() + 4 + len);
            }
        } else {
            let mut tmp = Vec::new();
            key_bytes(person, &mut tmp);
            put_str(&mut self.uid, &tmp);
        }
        let n = self.n;
        unsafe {
            *self.day.get_unchecked_mut(n) = day;
            *self.ses.get_unchecked_mut(n) = sessions as i32;
            *self.evs.get_unchecked_mut(n) = events as i32;
            *self.rev.get_unchecked_mut(n) = revenue;
        }
        if let Some(name) = top {
            self.defs[n / 8] |= 1 << (n % 8);
            put_str(&mut self.top, name);
        }
        self.n = n + 1;
        if self.n == GROUP_ROWS {
            self.flush();
        }
    }
    fn flush(&mut self) {
        let n = self.n;
        if n == 0 {
            return;
        }
        if self.w.is_none() {
            let f = std::fs::File::create(&self.path).expect("create user_daily part");
            self.w = Some(parquet::Writer::new(f, parquet::USER_DAILY).unwrap());
        }
        self.w
            .as_mut()
            .unwrap()
            .row_group(
                n,
                &[
                    parquet::ColData { values: &self.uid, defs: None },
                    parquet::ColData { values: as_bytes(&self.day[..n]), defs: None },
                    parquet::ColData { values: as_bytes(&self.ses[..n]), defs: None },
                    parquet::ColData { values: as_bytes(&self.evs[..n]), defs: None },
                    parquet::ColData { values: as_bytes(&self.rev[..n]), defs: None },
                    parquet::ColData { values: &self.top, defs: Some(&self.defs) },
                ],
            )
            .unwrap();
        self.total += n;
        self.n = 0;
        self.uid.clear();
        self.top.clear();
        self.defs.iter_mut().for_each(|b| *b = 0);
    }
    fn finish(mut self) -> usize {
        self.flush();
        if let Some(w) = self.w.take() {
            w.finish().unwrap();
        }
        self.total
    }
}

struct Scratch {
    bits: Vec<u64>,
    bmin: Vec<u32>,
    bmax: Vec<u32>,
    bcnt: Vec<u32>,
    bsess: Vec<u32>,
    sess: Vec<Sess>,
    pur: Vec<(u32, u32)>,
    clk: Vec<(u32, u32, u64)>,
    cnt: Vec<u32>,
    since: Vec<(u32, u32)>,
    posn: Vec<u32>,
    active: Vec<u32>,
    dso: Vec<u32>,
    dcat: Vec<i64>,
    camp: Vec<[i64; 2]>, // (sessions | purchasing << 32, revenue) per (campaign, day)
    asst: Vec<u32>,      // assisted sessions per (campaign, day)
    #[allow(dead_code)]
    tsc: [u64; 8],
}

fn sessionize_worker(ctx: &Ctx, parsed: &[Parsed], next: &AtomicUsize, order: &[u32], ud_dir: &Path, tid: usize) -> (Vec<[i64; 4]>, usize) {
    let np = 1usize << ctx.pbits;
    let mut sc = Scratch {
        bits: vec![0; ctx.nbins / 64 + 2],
        bmin: vec![0; ctx.nbins + 64],
        bmax: vec![0; ctx.nbins + 64],
        bcnt: vec![0; ctx.nbins + 64],
        bsess: vec![0; ctx.nbins + 64],
        sess: Vec::new(),
        pur: Vec::new(),
        clk: Vec::new(),
        cnt: vec![0; ctx.ncamp],
        since: vec![(0, 0); ctx.ncamp],
        posn: vec![0; ctx.ncamp],
        active: Vec::new(),
        dso: vec![0; ctx.nd + 2],
        dcat: vec![0; ctx.nd * ctx.ncats],
        camp: vec![[0i64; 2]; (ctx.ncamp + 1) * ctx.nd],
        asst: vec![0u32; (ctx.ncamp + 1) * ctx.nd],
        tsc: [0; 8],
    };
    let mut out = UdWriter::new(ud_dir.join(format!("part-{tid:03}.parquet")));
    let mut ev: Vec<ER> = Vec::new();
    let mut ck: Vec<CR> = Vec::new();
    let (mut ecount, mut ccount, mut pos) = (Vec::<u32>::new(), Vec::<u32>::new(), Vec::<u32>::new());
    let mut tm = [0f64; 2];
    loop {
        let k = next.fetch_add(1, Relaxed);
        if k >= np {
            break;
        }
        let p = order[k] as usize;
        let q0 = Instant::now();
        let n_all: usize = parsed.iter().map(|t| t.es.len(p)).sum();
        if n_all == 0 {
            continue;
        }
        let nsub = (n_all / 8).next_power_of_two().clamp(16, 1 << 16);
        let sbits = nsub.trailing_zeros();
        let pb = ctx.pbits;
        let sub = move |person: u64| ((person.wrapping_mul(K1) << pb) >> (64 - sbits)) as usize;
        // events: histogram, then scatter (dead copies dropped)
        ecount.clear();
        ecount.resize(nsub + 1, 0);
        for t in parsed {
            t.es.for_slices(p, |sl| {
                for e in sl {
                    if e.pay != DEAD {
                        ecount[sub(e.person) + 1] += 1;
                    }
                }
            });
        }
        for i in 0..nsub {
            ecount[i + 1] += ecount[i];
        }
        let n = ecount[nsub] as usize;
        ev.clear();
        ev.reserve(n);
        pos.clear();
        pos.extend_from_slice(&ecount[..nsub]);
        let d = ev.as_mut_ptr();
        for t in parsed {
            t.es.for_slices(p, |sl| {
                for e in sl {
                    if e.pay != DEAD {
                        let s = sub(e.person);
                        unsafe {
                            d.add(pos[s] as usize).write(*e);
                        }
                        pos[s] += 1;
                    }
                }
            });
        }
        unsafe { ev.set_len(n) };
        // clicks: same
        ccount.clear();
        ccount.resize(nsub + 1, 0);
        for t in parsed {
            t.cs.for_slices(p, |sl| {
                for c in sl {
                    ccount[sub(c.person) + 1] += 1;
                }
            });
        }
        for i in 0..nsub {
            ccount[i + 1] += ccount[i];
        }
        let nc = ccount[nsub] as usize;
        ck.clear();
        ck.reserve(nc);
        pos.clear();
        pos.extend_from_slice(&ccount[..nsub]);
        let d = ck.as_mut_ptr();
        for t in parsed {
            t.cs.for_slices(p, |sl| {
                for c in sl {
                    let s = sub(c.person);
                    unsafe {
                        d.add(pos[s] as usize).write(*c);
                    }
                    pos[s] += 1;
                }
            });
        }
        unsafe { ck.set_len(nc) };
        let q1 = Instant::now();
        for s in 0..nsub {
            let mut es = &mut ev[ecount[s] as usize..ecount[s + 1] as usize];
            let cs = &ck[ccount[s] as usize..ccount[s + 1] as usize];
            while !es.is_empty() {
                let person = es[0].person;
                let mut m = 1;
                for i in 1..es.len() {
                    if es[i].person == person {
                        es.swap(i, m);
                        m += 1;
                    }
                }
                let (mine, rest) = es.split_at_mut(m);
                do_person(ctx, person, mine, cs, &mut sc, &mut out);
                es = rest;
            }
        }
        tm[0] += (q1 - q0).as_secs_f64();
        tm[1] += q1.elapsed().as_secs_f64();
    }
    let total = out.finish();
    if prof() {
        eprintln!("  sess thread: scatter {:.1} persons {:.1} ms | Mcycles bins {} sessions {} clicks {} walk {} | persons {} sessions {} clicks {}", tm[0] * 1e3, tm[1] * 1e3, sc.tsc[0] / 1_000_000, sc.tsc[1] / 1_000_000, sc.tsc[2] / 1_000_000, sc.tsc[3] / 1_000_000, sc.tsc[4], sc.tsc[5], sc.tsc[6]);
    }
    let merged: Vec<[i64; 4]> = sc.camp.iter().zip(&sc.asst).map(|(c, &a)| [c[0] & 0xFFFF_FFFF, c[0] >> 32, c[1], a as i64]).collect();
    (merged, total)
}

#[inline(always)]
fn do_person(ctx: &Ctx, person: u64, evs: &[ER], cks: &[CR], sc: &mut Scratch, out: &mut UdWriter) {
    #[cfg(feature = "prof")]
    {
        let t0 = tsc();
        let (lo_w, hi_w) = person_bins(evs, sc);
        let t1 = tsc();
        person_sessions(ctx, lo_w, hi_w, sc);
        let t2 = tsc();
        person_clicks(person, cks, sc);
        let t3 = tsc();
        person_walk(ctx, person, sc, out);
        let t4 = tsc();
        sc.tsc[0] += t1 - t0;
        sc.tsc[1] += t2 - t1;
        sc.tsc[2] += t3 - t2;
        sc.tsc[3] += t4 - t3;
        sc.tsc[4] += 1;
        sc.tsc[5] += sc.sess.len() as u64;
        sc.tsc[6] += sc.clk.len() as u64;
        return;
    }
    #[allow(unreachable_code)]
    {
        let (lo_w, hi_w) = person_bins(evs, sc);
        person_sessions(ctx, lo_w, hi_w, sc);
        person_clicks(person, cks, sc);
        person_walk(ctx, person, sc, out);
    }
}

#[cfg(feature = "prof")]
#[inline(always)]
fn tsc() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        return std::arch::x86_64::_rdtsc();
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        return (sys::thread_cpu_ms() * 1e6) as u64;
    }
}

/// Half-hour bins: first, last and count of the events in each.
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn person_bins(evs: &[ER], sc: &mut Scratch) -> (usize, usize) {
    let (mut lo_w, mut hi_w) = (usize::MAX, 0usize);
    sc.pur.clear();
    for e in evs {
        let b = (e.ts / GAP) as usize;
        let (w, m) = (b >> 6, 1u64 << (b & 63));
        unsafe {
            if *sc.bits.get_unchecked(w) & m == 0 {
                *sc.bits.get_unchecked_mut(w) |= m;
                *sc.bmin.get_unchecked_mut(b) = e.ts;
                *sc.bmax.get_unchecked_mut(b) = e.ts;
                *sc.bcnt.get_unchecked_mut(b) = 1;
                lo_w = lo_w.min(w);
                hi_w = hi_w.max(w);
            } else {
                let mn = sc.bmin.get_unchecked_mut(b);
                *mn = (*mn).min(e.ts);
                let mx = sc.bmax.get_unchecked_mut(b);
                *mx = (*mx).max(e.ts);
                *sc.bcnt.get_unchecked_mut(b) += 1;
            }
        }
        if e.pay != 0 {
            sc.pur.push((e.ts, e.pay));
        }
    }
    (lo_w, hi_w)
}

/// Sessions: neighbouring bins join when the gap between them is at most 30
/// minutes; bins further apart are always more than 30 minutes apart.
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn person_sessions(ctx: &Ctx, lo_w: usize, hi_w: usize, sc: &mut Scratch) {
    let ncats = ctx.ncats;
    sc.sess.clear();
    let mut prev_b = 0usize;
    let mut last = 0u32;
    let mut di = 0u32;
    let mut dend = ctx.day_end0;
    for w in lo_w..=hi_w {
        let mut m = sc.bits[w];
        sc.bits[w] = 0;
        while m != 0 {
            let b = w * 64 + m.trailing_zeros() as usize;
            m &= m - 1;
            let mn = sc.bmin[b];
            if !sc.sess.is_empty() && b == prev_b + 1 && mn - last <= GAP {
                sc.sess.last_mut().unwrap().events += sc.bcnt[b];
            } else {
                while mn as i64 >= dend {
                    di += 1;
                    dend += DAY;
                }
                sc.sess.push(Sess { start: mn, events: sc.bcnt[b], rev: 0, di, purchases: 0 });
            }
            sc.bsess[b] = (sc.sess.len() - 1) as u32;
            last = sc.bmax[b];
            prev_b = b;
        }
    }
    for &(ts, pay) in &sc.pur {
        let s = &mut sc.sess[sc.bsess[(ts / GAP) as usize] as usize];
        let price = (pay & 0xFF_FFFF) as i64;
        s.rev += price;
        s.purchases += 1;
        sc.dcat[s.di as usize * ncats + (pay >> 24) as usize] += price;
    }
}

/// This person's clicks, by time.
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn person_clicks(person: u64, cks: &[CR], sc: &mut Scratch) {
    sc.clk.clear();
    for c in cks {
        if c.person == person {
            sc.clk.push((c.ts, c.camp, c.ck));
        }
    }
    if sc.clk.len() > 1 {
        sc.clk.sort_unstable_by_key(|c| c.0);
    }
}

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn person_walk(ctx: &Ctx, person: u64, sc: &mut Scratch, out: &mut UdWriter) {
    let nd = ctx.nd;
    let cl = &sc.clk;
    let has_clicks = !cl.is_empty();
    // 5. walk the sessions
    let organic = ctx.ncamp;
    let nsess = sc.sess.len() as u32;
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut filled = sc.sess[0].di as usize;
    if has_clicks {
        for d in 0..=filled {
            sc.dso[d] = 0;
        }
    }
    let mut cur = sc.sess[0].di;
    let (mut d_sessions, mut d_events, mut d_rev, mut d_pur) = (0u32, 0u32, 0i64, 0u32);
    for ord in 0..nsess {
        let s = sc.sess[ord as usize];
        if s.di != cur {
            flush_day(ctx, person, cur, d_sessions, d_events, d_rev, d_pur, &mut sc.dcat, out);
            cur = s.di;
            d_sessions = 0;
            d_events = 0;
            d_rev = 0;
            d_pur = 0;
        }
        d_sessions += 1;
        d_events += s.events;
        d_rev += s.rev;
        d_pur += s.purchases;
        let di = s.di as usize;
        let mut credited = organic;
        if has_clicks {
            if di > filled {
                for d in filled + 1..=di {
                    sc.dso[d] = ord;
                }
                filled = di;
            }
            while hi < cl.len() && cl[hi].0 <= s.start {
                let c = cl[hi].1 as usize;
                if sc.cnt[c] == 0 {
                    sc.since[c] = (ord, di as u32);
                    sc.posn[c] = sc.active.len() as u32;
                    sc.active.push(c as u32);
                }
                sc.cnt[c] += 1;
                hi += 1;
            }
            let floor = s.start as i64 - WEEK;
            while lo < hi && (cl[lo].0 as i64) < floor {
                let c = cl[lo].1 as usize;
                sc.cnt[c] -= 1;
                if sc.cnt[c] == 0 {
                    assist(&mut sc.asst, &sc.dso, c, sc.since[c], ord, filled, nd);
                    let at = sc.posn[c] as usize;
                    let lastc = sc.active.pop().unwrap();
                    if lastc as usize != c {
                        sc.active[at] = lastc;
                        sc.posn[lastc as usize] = at as u32;
                    }
                }
                lo += 1;
            }
            if hi > lo {
                let lt = cl[hi - 1].0;
                let mut best = hi - 1;
                let mut k = hi - 1;
                while k > lo && cl[k - 1].0 == lt {
                    k -= 1;
                    if cmp_key(cl[k].2, cl[best].2) == Ordering::Less {
                        best = k;
                    }
                }
                credited = cl[best].1 as usize;
            }
        }
        let a = &mut sc.camp[credited * nd + di];
        a[0] += 1 + (((s.purchases > 0) as i64) << 32);
        a[1] += s.rev;
    }
    flush_day(ctx, person, cur, d_sessions, d_events, d_rev, d_pur, &mut sc.dcat, out);
    if has_clicks {
        for i in 0..sc.active.len() {
            let c = sc.active[i] as usize;
            assist(&mut sc.asst, &sc.dso, c, sc.since[c], nsess, filled, nd);
            sc.cnt[c] = 0;
        }
        sc.active.clear();
    }
}

/// Campaign `c` assisted the sessions `a..b` (ordinals); the first is on day `da`.
#[inline(always)]
fn assist(asst: &mut [u32], dso: &[u32], c: usize, since: (u32, u32), b: u32, filled: usize, nd: usize) {
    let (a, da) = since;
    for d in da as usize..=filled {
        let l = a.max(dso[d]);
        let h = if d + 1 > filled { b } else { b.min(dso[d + 1]) };
        if h > l {
            asst[c * nd + d] += h - l;
        }
    }
}

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn flush_day(ctx: &Ctx, person: u64, di: u32, sessions: u32, events: u32, revenue: i64, purchases: u32, dcat: &mut [i64], out: &mut UdWriter) {
    let mut top: Option<&[u8]> = None;
    if purchases > 0 {
        let row = &mut dcat[di as usize * ctx.ncats..(di as usize + 1) * ctx.ncats];
        let mut best = 0i64;
        let mut bi = 0usize;
        for (i, v) in row.iter_mut().enumerate() {
            if *v > best {
                best = *v;
                bi = i;
            }
            *v = 0;
        }
        top = Some(&ctx.lk.cat_names[bi]);
    }
    out.push(person, (ctx.day0 + di as i64) as i32, sessions, events, revenue, top);
}

fn put_str(v: &mut Vec<u8>, s: &[u8]) {
    v.extend_from_slice(&(s.len() as u32).to_le_bytes());
    v.extend_from_slice(s);
}

fn write_campaign_daily(path: &Path, ids: &[Vec<u8>], channels: &[Vec<u8>], tot: &[[i64; 4]], first_day: i64, nd: usize) {
    let (mut a, mut b, mut d, mut s, mut p, mut r, mut x) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut n = 0;
    for c in 0..=ids.len() {
        let (id, ch): (&[u8], &[u8]) = if c == ids.len() { (b"organic", b"organic") } else { (&ids[c], &channels[c]) };
        for di in 0..nd {
            let t = tot[c * nd + di];
            if t[0] == 0 && t[3] == 0 {
                continue;
            }
            n += 1;
            put_str(&mut a, id);
            put_str(&mut b, ch);
            d.extend_from_slice(&((first_day + di as i64) as i32).to_le_bytes());
            s.extend_from_slice(&t[0].to_le_bytes());
            p.extend_from_slice(&t[1].to_le_bytes());
            r.extend_from_slice(&t[2].to_le_bytes());
            x.extend_from_slice(&t[3].to_le_bytes());
        }
    }
    let f = std::fs::File::create(path).expect("create campaign_daily");
    let mut w = parquet::Writer::new(std::io::BufWriter::with_capacity(1 << 20, f), parquet::CAMPAIGN_DAILY).unwrap();
    w.row_group(
        n,
        &[
            parquet::ColData { values: &a, defs: None },
            parquet::ColData { values: &b, defs: None },
            parquet::ColData { values: &d, defs: None },
            parquet::ColData { values: &s, defs: None },
            parquet::ColData { values: &p, defs: None },
            parquet::ColData { values: &r, defs: None },
            parquet::ColData { values: &x, defs: None },
        ],
    )
    .unwrap();
    w.finish().unwrap();
}


#[cfg(feature = "selfbench")]
static PHASES: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());

/// Makes the IDs of copy `k` of a file distinct: the character after the
/// leading `e`, `u` or `k` of a field is rotated by `k` in the base-36 alphabet.
#[cfg(feature = "selfbench")]
fn tile_ids(buf: &mut [u8], k: u8) {
    if k == 0 {
        return;
    }
    let n = buf.len();
    let mut i = match buf.iter().position(|&c| c == b'\n') {
        Some(h) => h + 1,
        None => return,
    };
    let mut at_start = true;
    while i + 2 < n {
        let c = buf[i];
        if at_start {
            let j = if c == b'"' { i + 1 } else { i };
            if matches!(buf[j], b'e' | b'u' | b'k') {
                let d = buf[j + 1];
                let v = match d {
                    b'0'..=b'9' => Some(d - b'0'),
                    b'a'..=b'z' => Some(d - b'a' + 10),
                    _ => None,
                };
                if let Some(v) = v {
                    let r = (v + k) % 36;
                    buf[j + 1] = if r < 10 { b'0' + r } else { b'a' + r - 10 };
                }
            }
        }
        at_start = c == b',' || c == b'\n';
        i += 1;
    }
}

/// Times variants of the parse phase on this machine, and the whole pipeline on
/// the inputs tiled several times with distinct IDs. Returns text rows.
#[cfg(feature = "selfbench")]
#[allow(clippy::too_many_arguments)]
fn self_bench(ctx: &Ctx, maps: &[sys::Mapped], files: &[std::fs::File], nev: usize, nck: usize, nthreads: usize, ws: i64, we: i64, data: &Path, out_dir: &Path) -> Vec<String> {
    let ev_bytes: usize = maps[..nev].iter().map(|m| m.len).sum();
    let ck_bytes: usize = maps[nev..nev + nck].iter().map(|m| m.len).sum();
    let reps = ((1usize << 30) / ev_bytes.max(1)).clamp(1, 64);
    let mut out: Vec<(String, f64)> = Vec::new();

    // 1. the whole pipeline on a tiled copy held in anonymous memory
    let mut tiled: Vec<sys::Mapped> = Vec::new();
    let t = Instant::now();
    for (lo, hi) in [(0, nev), (nev, nev + nck), (nev + nck, maps.len())] {
        for k in 0..reps {
            for m in &maps[lo..hi] {
                let p = sys::anon(m.len + 4096, true);
                let buf = unsafe { std::slice::from_raw_parts_mut(p, m.len) };
                buf.copy_from_slice(m.bytes());
                tile_ids(buf, k as u8);
                tiled.push(sys::Mapped { ptr: p, len: m.len });
            }
        }
    }
    out.push(("mk".into(), t.elapsed().as_secs_f64() * 1e3));
    let bdir = out_dir.join("_bench");
    for round in 0..2 {
        let _ = std::fs::remove_dir_all(&bdir);
        std::fs::create_dir_all(bdir.join("user_daily")).unwrap();
        std::fs::create_dir_all(bdir.join("campaign_daily")).unwrap();
        let t = Instant::now();
        let marks: std::sync::Mutex<Vec<(String, f64)>> = std::sync::Mutex::new(Vec::new());
        let d2 = data.to_path_buf();
        pipeline(&tiled, &[], nev * reps, nck * reps, ws, we, move || load_lookups(&d2), &bdir, nthreads, false, &|w: &str| {
            let name: String = w.chars().take_while(|c| c.is_ascii_alphabetic() || *c == ' ').take(7).collect::<String>().replace(' ', "");
            marks.lock().unwrap().push((name, t.elapsed().as_secs_f64() * 1e3));
        });
        for (n, v) in marks.into_inner().unwrap() {
            out.push((format!("T{round}{n}"), v));
        }
    }
    let t = Instant::now();
    let _ = std::fs::remove_dir_all(&bdir);
    out.push(("rmout".into(), t.elapsed().as_secs_f64() * 1e3));
    for m in &tiled {
        sys::unmap(m.ptr, (m.len + 4096 + 4095) & !4095);
    }

    // 2. fresh mappings of the real inputs: first-touch and unmap cost
    {
        let ev_files = csv_files(&data.join("events"));
        let fresh: Vec<sys::Mapped> = ev_files.iter().map(|p| sys::Mapped::open(p, false).unwrap().0).collect();
        let mut ch = Vec::new();
        make_chunks(&fresh, 0, 0, 1 << 20, &mut ch);
        let pp = PP { sub: 16 << 10, pbits: 10, bbits: 10, mode: 3, pread: false, huge: true, block: BLOCK, threads: nthreads, pf: PF, arena_cap: 4096 };
        let t = Instant::now();
        parse_phase(ctx, &fresh, files, &ch, &pp);
        out.push(("ftouch".into(), t.elapsed().as_secs_f64() * 1e3));
        let t = Instant::now();
        let next = AtomicUsize::new(0);
        run_threads(nthreads, |_| loop {
            let k = next.fetch_add(1, Relaxed);
            let Some(m) = fresh.get(k) else { break };
            if m.len > 0 {
                sys::unmap(m.ptr, (m.len + 4095) & !4095);
            }
        });
        out.push(("funmap".into(), t.elapsed().as_secs_f64() * 1e3));
    }

    // 3. parse-phase variants on the real inputs replayed `reps` times
    let chunks_of = |size: usize| -> Vec<Chunk> {
        let mut one = Vec::new();
        make_chunks(&maps[..nev], 0, 0, size, &mut one);
        make_chunks(&maps[nev..nev + nck], nev, 1, size, &mut one);
        let mut all = Vec::with_capacity(one.len() * reps);
        for _ in 0..reps {
            all.extend_from_slice(&one);
        }
        all
    };
    let arena_cap = (ev_bytes / 36 * 32 + ck_bytes / 20 * 24) * reps + (64 << 20);
    let base = PP { sub: 16 << 10, pbits: 10, bbits: 10, mode: 0, pread: false, huge: true, block: BLOCK, threads: nthreads, pf: PF, arena_cap };
    let c1m = chunks_of(1 << 20);
    let c256 = chunks_of(256 << 10);
    let mut run = |name: &str, chunks: &[Chunk], pp: PP| {
        let mut best = f64::MAX;
        for _ in 0..2 {
            let t = Instant::now();
            let mut parsed = parse_phase(ctx, maps, files, chunks, &pp);
            best = best.min(t.elapsed().as_secs_f64() * 1e3);
            for p in parsed.iter_mut() {
                p.arena.free();
            }
        }
        out.push((name.to_string(), best));
    };
    run("base", &c1m, base);
    run("touch", &c1m, PP { mode: 3, ..base });
    run("idx", &c1m, PP { mode: 1, ..base });
    run("prs", &c1m, PP { mode: 2, ..base });
    run("s4k", &c1m, PP { sub: 4 << 10, ..base });
    run("s64k", &c1m, PP { sub: 64 << 10, ..base });
    run("s1m", &c1m, PP { sub: 1 << 20, ..base });
    run("b8", &c1m, PP { pbits: 8, bbits: 8, ..base });
    run("b9", &c1m, PP { pbits: 9, bbits: 9, ..base });
    run("b11", &c1m, PP { pbits: 11, bbits: 11, ..base });
    run("b12", &c1m, PP { pbits: 12, bbits: 12, ..base });
    run("pr1m", &c1m, PP { pread: true, ..base });
    run("pr256", &c256, PP { pread: true, ..base });
    run("c256", &c256, base);
    run("nohuge", &c1m, PP { huge: false, ..base });
    run("t4", &c1m, PP { threads: (nthreads / 2).max(1), ..base });
    run("blk64", &c1m, PP { block: 64, ..base });
    run("blk1k", &c1m, PP { block: 1024, ..base });
    run("pf0", &c1m, PP { pf: usize::MAX / 2, ..base });
    run("pf32", &c1m, PP { pf: 32, ..base });
    run("base2", &c1m, base);
    // fixed costs
    let t = Instant::now();
    for _ in 0..20 {
        run_threads(nthreads, |_| 0u8);
    }
    let spawn = t.elapsed().as_secs_f64() * 1e3 / 20.0;
    let t = Instant::now();
    let blob = vec![1u8; 1 << 20];
    for i in 0..8 {
        let _ = std::fs::write(out_dir.join(format!("_b{i}.bin")), &blob);
    }
    let create = t.elapsed().as_secs_f64() * 1e3 / 8.0;
    for i in 0..8 {
        let _ = std::fs::remove_file(out_dir.join(format!("_b{i}.bin")));
    }
    let mut rows = Vec::new();
    let mut cur = format!("!F reps{reps} spawn8={spawn:.2} create1M={create:.2} |");
    for (n, v) in &out {
        let item = format!(" {n}={v:.0}");
        if cur.len() + item.len() > 215 {
            rows.push(std::mem::take(&mut cur));
            cur = format!("!G{}", rows.len());
        }
        cur.push_str(&item);
    }
    rows.push(cur);
    rows.push(format!("!E {}", PHASES.lock().unwrap()));
    rows
}

#[cfg(feature = "selfbench")]
fn write_extra_rows(ud_dir: &Path, day0: i64, rows: &[String]) {
    let mut uid = Vec::new();
    for r in rows {
        put_str(&mut uid, r.as_bytes());
    }
    let n = rows.len();
    let day: Vec<i32> = vec![day0 as i32; n];
    let ones: Vec<i32> = vec![1; n];
    let rev: Vec<i64> = vec![0; n];
    let defs = vec![0u8; n / 8 + 1];
    let f = std::fs::File::create(ud_dir.join("part-zzz.parquet")).unwrap();
    let mut w = parquet::Writer::new(f, parquet::USER_DAILY).unwrap();
    w.row_group(
        n,
        &[
            parquet::ColData { values: &uid, defs: None },
            parquet::ColData { values: as_bytes(&day), defs: None },
            parquet::ColData { values: as_bytes(&ones), defs: None },
            parquet::ColData { values: as_bytes(&ones), defs: None },
            parquet::ColData { values: as_bytes(&rev), defs: None },
            parquet::ColData { values: &[], defs: Some(&defs) },
        ],
    )
    .unwrap();
    w.finish().unwrap();
}
