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
mod parquet;
mod scan;
mod sys;
mod ts;

use buf::{Arena, Stream};
use ids::{cmp_key, id_key, id_key_slice, key_bytes};
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

const K1: u64 = 0x9E37_79B9_7F4A_7C15;
const K2: u64 = 0xD6E8_FEB8_6659_FD93;
const DAY: i64 = 86_400_000;
const GAP: u32 = 1_800_000;
const WEEK: i64 = 7 * DAY;

/// Event-key buckets (dedup) and person partitions.
const BBITS: u32 = 9;
const NB: usize = 1 << BBITS;
const PBITS: u32 = 9;
const NP: usize = 1 << PBITS;
const IDXBITS: u32 = 32 - PBITS;
const NONE: u32 = u32::MAX;

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
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
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

/// A parsed event copy, chunk-local. kind 0 = the copy is invalid.
#[derive(Clone, Copy)]
#[repr(C)]
struct LR {
    ek: u64,
    uk: u64,
    ingest: u32,
    ts: u32,
    price: u32,
    cat: u16,
    kind: u8,
    _p: u8,
}

/// A valid event copy in its person partition. kind 0 = killed by dedup.
#[derive(Clone, Copy)]
#[repr(C)]
struct ER {
    person: u64,
    ts: u32,
    price: u32,
    cat: u16,
    kind: u8,
    _p: u8,
    _q: u32,
}

/// Dedup record: event key, ingest (seconds, offset), where its ER lives.
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

struct UdRow {
    person: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    top: i32,
}

struct Ctx {
    base: i64, // window_start - 7 days; every ts is stored relative to it
    base_s: i64,
    ws_rel: u32,
    we_rel: u32,
    first_day: i64,
    ndays: usize,
    prod: U64Map,
    unknown_cat: u16,
    ncats: usize,
    camp: U64Map,
    ncamp: usize,
    person: U64Map,
}

struct Parsed {
    es: Vec<Stream<ER>>,
    ss: Vec<Stream<SR>>,
    cs: Vec<Stream<CR>>,
    _arena: Arena,
}

fn log(t0: &Instant, what: &str) {
    eprintln!("[{:8.3} ms] {what}", t0.elapsed().as_secs_f64() * 1e3);
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
    let ws = mval("window_start");
    let we = mval("window_end");
    let base = ws - WEEK;
    assert!(we > ws && we - base < (1i64 << 32), "window too long");
    let first_day = base.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - first_day + 1) as usize;

    // --- products and categories
    let products = small_csv::<2>(&data.join("products.csv"));
    let mut cat_names: Vec<Vec<u8>> = products.iter().map(|r| r[1].clone()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    assert!(cat_names.len() < 65_000);
    let cat_rank = |name: &[u8]| cat_names.binary_search_by(|c| c.as_slice().cmp(name)).unwrap() as u16;
    let unknown_cat = cat_rank(b"unknown");
    let mut prod = U64Map::new(products.len());
    for r in &products {
        if !r[0].is_empty() {
            prod.insert(id_key_slice(&r[0]), cat_rank(&r[1]) as u64);
        }
    }
    // --- campaigns
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
    let ncamp = camp_ids.len();

    // --- map every input file
    let ev_files = csv_files(&data.join("events"));
    let ck_files = csv_files(&data.join("clicks"));
    let ln_files = csv_files(&data.join("links"));
    let mut maps: Vec<sys::Mapped> = Vec::new();
    for p in ev_files.iter().chain(&ck_files).chain(&ln_files) {
        maps.push(sys::Mapped::open(p, false).expect("mmap"));
    }
    let (nev, nck) = (ev_files.len(), ck_files.len());
    let ev_bytes: usize = maps[..nev].iter().map(|m| m.len).sum();
    let ck_bytes: usize = maps[nev..nev + nck].iter().map(|m| m.len).sum();
    log(&t0, &format!("lookups: {} products, {} cats, {} campaigns; {} event files {} B, {} click files {} B, {} link files", products.len(), cat_names.len(), ncamp, nev, ev_bytes, nck, ck_bytes, ln_files.len()));

    // --- links
    let mut lchunks = Vec::new();
    make_chunks(&maps[nev + nck..], nev + nck, 2, 1 << 20, &mut lchunks);
    let next = AtomicUsize::new(0);
    let pairs: Vec<Vec<(u64, u64)>> = run_threads(nthreads, |_| {
        let mut v = Vec::new();
        let mut f = [(0usize, 0usize); 2];
        loop {
            let k = next.fetch_add(1, Relaxed);
            let Some(c) = lchunks.get(k) else { break };
            let b = maps[c.file as usize].bytes();
            scan::scan_lines(b, c.start, c.end, |ls, le, commas, nc, qc| {
                if nc != 1 || !scan::fields::<2>(b, ls, le, commas, qc, &mut f) {
                    return;
                }
                if f[0].0 == f[0].1 || f[1].0 == f[1].1 {
                    return;
                }
                v.push((key_at(b, f[0].0, f[0].1), key_at(b, f[1].0, f[1].1)));
            });
        }
        v
    });
    let npairs: usize = pairs.iter().map(|v| v.len()).sum();
    let mut node = U64Map::new(npairs * 2);
    let mut keys: Vec<u64> = Vec::with_capacity(npairs * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(npairs * 2);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for v in &pairs {
        for (n, &(a, b)) in v.iter().enumerate() {
            if let Some(&(c, d)) = v.get(n + 8) {
                node.prefetch(c);
                node.prefetch(d);
            }
            let mut ids = [0u32; 2];
            for (i, k) in [a, b].into_iter().enumerate() {
                let n = keys.len() as u64;
                let id = node.insert(k, n);
                if id == n {
                    keys.push(k);
                    parent.push(n as u32);
                }
                ids[i] = id as u32;
            }
            let (ra, rb) = (find(&mut parent, ids[0]), find(&mut parent, ids[1]));
            if ra != rb {
                let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
                parent[hi as usize] = lo;
            }
        }
    }
    let nn = keys.len();
    let mut minkey: Vec<u64> = keys.clone();
    let mut root = vec![0u32; nn];
    for i in 0..nn {
        let r = find(&mut parent, i as u32);
        root[i] = r;
        if cmp_key(keys[i], minkey[r as usize]) == Ordering::Less {
            minkey[r as usize] = keys[i];
        }
    }
    let mut person = U64Map::new(nn);
    for i in 0..nn {
        let m = minkey[root[i] as usize];
        if m != keys[i] {
            person.insert(keys[i], m);
        }
    }
    drop(pairs);
    drop(node);
    log(&t0, &format!("links: {npairs} pairs, {nn} ids"));

    let ctx = Ctx {
        base,
        base_s: base.div_euclid(1000),
        ws_rel: (ws - base) as u32,
        we_rel: (we - base) as u32,
        first_day,
        ndays,
        prod,
        unknown_cat,
        ncats: cat_names.len(),
        camp,
        ncamp,
        person,
    };

    // --- parse events and clicks
    let mut chunks = Vec::new();
    make_chunks(&maps[..nev], 0, 0, 1 << 20, &mut chunks);
    make_chunks(&maps[nev..nev + nck], nev, 1, 1 << 20, &mut chunks);
    let arena_chunk = ((ev_bytes / 2 + ck_bytes) / nthreads / 4).max(8 << 20);
    let next = AtomicUsize::new(0);
    let parsed: Vec<Parsed> = run_threads(nthreads, |_| {
        let mut arena = Arena::new(arena_chunk);
        let mut es: Vec<Stream<ER>> = (0..NP).map(|_| Stream::new()).collect();
        let mut ss: Vec<Stream<SR>> = (0..NB).map(|_| Stream::new()).collect();
        let mut cs: Vec<Stream<CR>> = (0..NP).map(|_| Stream::new()).collect();
        let mut lr: Vec<LR> = Vec::with_capacity(1 << 16);
        let mut lc: Vec<LC> = Vec::with_capacity(1 << 16);
        loop {
            let k = next.fetch_add(1, Relaxed);
            let Some(c) = chunks.get(k) else { break };
            let b = maps[c.file as usize].bytes();
            if c.kind == 0 {
                lr.clear();
                parse_events(&ctx, b, c.start, c.end, &mut lr);
                emit_events(&ctx, &lr, &mut es, &mut ss, &mut arena);
            } else {
                lc.clear();
                parse_clicks(&ctx, b, c.start, c.end, &mut lc);
                emit_clicks(&ctx, &lc, &mut cs, &mut arena);
            }
        }
        Parsed { es, ss, cs, _arena: arena }
    });
    let n_ev: usize = parsed.iter().map(|t| t.es.iter().map(|s| s.len()).sum::<usize>()).sum();
    let n_sr: usize = parsed.iter().map(|t| t.ss.iter().map(|s| s.len()).sum::<usize>()).sum();
    let n_ck: usize = parsed.iter().map(|t| t.cs.iter().map(|s| s.len()).sum::<usize>()).sum();
    log(&t0, &format!("parsed: {n_sr} event copies ({n_ev} valid), {n_ck} valid clicks"));

    // --- dedup: kill every copy but the earliest-ingested one
    let next = AtomicUsize::new(0);
    let killed: Vec<usize> = run_threads(nthreads, |_| {
        let mut table: Vec<(u64, u32, u32, u32)> = Vec::new();
        let mut killed = 0usize;
        loop {
            let bkt = next.fetch_add(1, Relaxed);
            if bkt >= NB {
                break;
            }
            let n: usize = parsed.iter().map(|t| t.ss[bkt].len()).sum();
            if n == 0 {
                continue;
            }
            let cap = (n * 2).next_power_of_two().max(16);
            let shift = 64 - cap.trailing_zeros();
            let mask = cap - 1;
            table.clear();
            table.resize(cap, (0, 0, 0, 0));
            let kill = |t: u32, loc: u32| {
                if loc != NONE {
                    let part = (loc >> IDXBITS) as usize;
                    let idx = (loc & ((1 << IDXBITS) - 1)) as usize;
                    unsafe { (*parsed[t as usize].es[part].ptr(idx)).kind = 0 };
                }
            };
            for (t, th) in parsed.iter().enumerate() {
                th.ss[bkt].for_slices(|sl| {
                    for r in sl {
                        let mut i = (r.ek.wrapping_mul(K2) >> shift) as usize;
                        loop {
                            let slot = unsafe { table.get_unchecked_mut(i) };
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
                            i = (i + 1) & mask;
                        }
                    }
                });
            }
        }
        killed
    });
    log(&t0, &format!("dedup: {} duplicate copies", killed.iter().sum::<usize>()));

    // --- sessionize per person partition
    let next = AtomicUsize::new(0);
    let results: Vec<(Vec<UdRow>, Vec<[i64; 4]>)> = run_threads(nthreads, |_| sessionize_worker(&ctx, &parsed, &next));
    log(&t0, "sessionized");

    // --- write outputs
    let ud_dir = out.join("user_daily");
    let cd_dir = out.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd_dir).expect("mkdir campaign_daily");
    let mut camp_tot = vec![[0i64; 4]; (ncamp + 1) * ndays];
    for (_, c) in &results {
        for (a, b) in camp_tot.iter_mut().zip(c) {
            for j in 0..4 {
                a[j] += b[j];
            }
        }
    }
    let total_ud: usize = results.iter().map(|r| r.0.len()).sum();
    std::thread::scope(|s| {
        for (t, (rows, _)) in results.iter().enumerate() {
            if rows.is_empty() && !(total_ud == 0 && t == 0) {
                continue;
            }
            let path = ud_dir.join(format!("part-{t:03}.parquet"));
            let cat_names = &cat_names;
            s.spawn(move || write_user_daily(&path, rows, cat_names));
        }
        let path = cd_dir.join("part-000.parquet");
        let (camp_ids, camp_channels, camp_tot) = (&camp_ids, &camp_channels, &camp_tot);
        s.spawn(move || write_campaign_daily(&path, camp_ids, camp_channels, camp_tot, first_day, ndays));
    });
    log(&t0, &format!("wrote {total_ud} user_daily rows"));
    sys::exit_now(0);
}

#[inline(always)]
fn parse_events(ctx: &Ctx, b: &[u8], start: usize, end: usize, lr: &mut Vec<LR>) {
    let mut f = [(0usize, 0usize); 7];
    scan::scan_lines(b, start, end, |ls, le, commas, nc, qc| {
        if nc != 6 || !scan::fields::<7>(b, ls, le, commas, qc, &mut f) {
            return;
        }
        let (s0, e0) = f[0];
        if s0 == e0 {
            return;
        }
        let ek = key_at(b, s0, e0);
        let fld = |k: usize| unsafe { b.get_unchecked(f[k].0..f[k].1) };
        let ingest_ms = ts::parse_ts_slice(fld(3));
        let ingest = match ingest_ms {
            Some(ms) => (ms.div_euclid(1000) - ctx.base_s + (1i64 << 31)).clamp(0, u32::MAX as i64 - 1) as u32,
            None => u32::MAX,
        };
        let mut e = LR { ek, uk: 0, ingest, ts: 0, price: 0, cat: 0, kind: 0, _p: 0 };
        'v: {
            if f[1].0 == f[1].1 || f[5].0 == f[5].1 {
                break 'v;
            }
            let Some(t) = ts::parse_ts_slice(fld(2)) else { break 'v };
            let rel = t - ctx.base;
            if rel < ctx.ws_rel as i64 || rel >= ctx.we_rel as i64 {
                break 'v;
            }
            let kind = match fld(4) {
                b"view" => 1u8,
                b"cart" => 2,
                b"purchase" => 3,
                _ => break 'v,
            };
            if kind == 3 {
                let Some(p) = parse_price(fld(6)) else { break 'v };
                e.price = p;
            }
            match ingest_ms {
                Some(ms) if ms - t <= DAY => {}
                _ => break 'v,
            }
            if kind == 3 {
                let pk = key_at(b, f[5].0, f[5].1);
                e.cat = ctx.prod.get(pk).map(|c| c as u16).unwrap_or(ctx.unknown_cat);
            }
            e.ts = rel as u32;
            e.uk = key_at(b, f[1].0, f[1].1);
            e.kind = kind;
        }
        lr.push(e);
    });
}

const PF: usize = 16;

#[inline(always)]
fn emit_events(ctx: &Ctx, lr: &[LR], es: &mut [Stream<ER>], ss: &mut [Stream<SR>], arena: &mut Arena) {
    for (i, e) in lr.iter().enumerate() {
        if let Some(x) = lr.get(i + PF) {
            if x.kind != 0 {
                ctx.person.prefetch(x.uk);
            }
        }
        let mut loc = NONE;
        if e.kind != 0 {
            let person = ctx.person.get(e.uk).unwrap_or(e.uk);
            let part = top_bits(person, PBITS);
            let idx = unsafe { es.get_unchecked_mut(part) }.push(ER { person, ts: e.ts, price: e.price, cat: e.cat, kind: e.kind, _p: 0, _q: 0 }, arena);
            assert!(idx < (1 << IDXBITS), "partition overflow");
            loc = ((part as u32) << IDXBITS) | idx as u32;
        }
        unsafe { ss.get_unchecked_mut(top_bits(e.ek, BBITS)) }.push(SR { ek: e.ek, ingest: e.ingest, loc }, arena);
    }
}

#[inline(always)]
fn parse_clicks(ctx: &Ctx, b: &[u8], start: usize, end: usize, lc: &mut Vec<LC>) {
    let mut f = [(0usize, 0usize); 4];
    scan::scan_lines(b, start, end, |ls, le, commas, nc, qc| {
        if nc != 3 || !scan::fields::<4>(b, ls, le, commas, qc, &mut f) {
            return;
        }
        if f[0].0 == f[0].1 || f[1].0 == f[1].1 || f[2].0 == f[2].1 {
            return;
        }
        let Some(t) = ts::parse_ts_slice(unsafe { b.get_unchecked(f[3].0..f[3].1) }) else { return };
        let rel = t - ctx.base;
        if rel < 0 || rel >= ctx.we_rel as i64 {
            return;
        }
        let Some(camp) = ctx.camp.get(key_at(b, f[2].0, f[2].1)) else { return };
        lc.push(LC { uk: key_at(b, f[1].0, f[1].1), ck: key_at(b, f[0].0, f[0].1), ts: rel as u32, camp: camp as u32 });
    });
}

#[inline(always)]
fn emit_clicks(ctx: &Ctx, lc: &[LC], cs: &mut [Stream<CR>], arena: &mut Arena) {
    for (i, c) in lc.iter().enumerate() {
        if let Some(x) = lc.get(i + PF) {
            ctx.person.prefetch(x.uk);
        }
        let person = ctx.person.get(c.uk).unwrap_or(c.uk);
        unsafe { cs.get_unchecked_mut(top_bits(person, PBITS)) }.push(CR { person, ck: c.ck, ts: c.ts, camp: c.camp }, arena);
    }
}

/// Counting sort of `src` into `dst` by sub-bucket of the person hash.
fn bucketize<T: Copy>(src: &[T], dst: &mut Vec<T>, counts: &mut Vec<u32>, nsub: usize, sbits: u32, person: impl Fn(&T) -> u64) {
    counts.clear();
    counts.resize(nsub + 1, 0);
    let sub = |p: u64| ((p.wrapping_mul(K1) << PBITS) >> (64 - sbits)) as usize;
    for x in src {
        counts[sub(person(x)) + 1] += 1;
    }
    for i in 0..nsub {
        counts[i + 1] += counts[i];
    }
    dst.clear();
    dst.reserve(src.len());
    let mut pos: Vec<u32> = counts[..nsub].to_vec();
    let d = dst.as_mut_ptr();
    for x in src {
        let s = sub(person(x));
        unsafe { d.add(pos[s] as usize).write(*x) };
        pos[s] += 1;
    }
    unsafe { dst.set_len(src.len()) };
}

struct Acc {
    camp: Vec<[i64; 4]>, // sessions, purchasing, revenue, assisted per (campaign, day)
    rows: Vec<UdRow>,
    cnt: Vec<u32>,
    posn: Vec<u32>,
    active: Vec<u32>,
    cat_rev: Vec<i64>,
    touched: Vec<u16>,
}

fn sessionize_worker(ctx: &Ctx, parsed: &[Parsed], next: &AtomicUsize) -> (Vec<UdRow>, Vec<[i64; 4]>) {
    let mut acc = Acc {
        camp: vec![[0i64; 4]; (ctx.ncamp + 1) * ctx.ndays],
        rows: Vec::new(),
        cnt: vec![0u32; ctx.ncamp],
        posn: vec![0u32; ctx.ncamp],
        active: Vec::new(),
        cat_rev: vec![0i64; ctx.ncats],
        touched: Vec::new(),
    };
    let (mut ev0, mut ev, mut ck0, mut ck) = (Vec::<ER>::new(), Vec::<ER>::new(), Vec::<CR>::new(), Vec::<CR>::new());
    let (mut ecount, mut ccount) = (Vec::new(), Vec::new());
    loop {
        let p = next.fetch_add(1, Relaxed);
        if p >= NP {
            break;
        }
        ev0.clear();
        for t in parsed {
            t.es[p].for_slices(|sl| {
                for e in sl {
                    if e.kind != 0 {
                        ev0.push(*e);
                    }
                }
            });
        }
        ck0.clear();
        for t in parsed {
            t.cs[p].for_slices(|sl| ck0.extend_from_slice(sl));
        }
        if ev0.is_empty() {
            continue;
        }
        let nsub = (ev0.len() / 16).next_power_of_two().clamp(16, 1 << 16);
        let sbits = nsub.trailing_zeros();
        bucketize(&ev0, &mut ev, &mut ecount, nsub, sbits, |e| e.person);
        bucketize(&ck0, &mut ck, &mut ccount, nsub, sbits, |c| c.person);
        for s in 0..nsub {
            let es = &mut ev[ecount[s] as usize..ecount[s + 1] as usize];
            if es.is_empty() {
                continue;
            }
            let cs = &mut ck[ccount[s] as usize..ccount[s + 1] as usize];
            es.sort_unstable_by_key(|e| ((e.person as u128) << 32) | e.ts as u128);
            cs.sort_unstable_by_key(|c| ((c.person as u128) << 32) | c.ts as u128);
            sessionize(ctx, es, cs, &mut acc);
        }
    }
    (acc.rows, acc.camp)
}

/// Events and clicks of a set of persons, each sorted by (person, ts).
#[inline(always)]
fn sessionize(ctx: &Ctx, evs: &[ER], cks: &[CR], acc: &mut Acc) {
    let nd = ctx.ndays;
    let organic = ctx.ncamp;
    let mut ci = 0usize;
    let mut i = 0usize;
    while i < evs.len() {
        let person = evs[i].person;
        let mut j = i + 1;
        while j < evs.len() && evs[j].person == person {
            j += 1;
        }
        while ci < cks.len() && cks[ci].person < person {
            ci += 1;
        }
        let c0 = ci;
        while ci < cks.len() && cks[ci].person == person {
            ci += 1;
        }
        let cl = &cks[c0..ci];
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut cur_day = i64::MIN;
        let (mut d_sessions, mut d_events, mut d_rev) = (0u32, 0u32, 0i64);
        let mut s = i;
        while s < j {
            let start = evs[s].ts;
            let mut e = s + 1;
            while e < j && evs[e].ts - evs[e - 1].ts <= GAP {
                e += 1;
            }
            let day = (ctx.base + start as i64).div_euclid(DAY);
            let di = (day - ctx.first_day) as usize;
            if day != cur_day {
                if d_sessions > 0 {
                    let r = flush_day(person, cur_day, d_sessions, d_events, d_rev, &mut acc.cat_rev, &mut acc.touched);
                    acc.rows.push(r);
                }
                cur_day = day;
                d_sessions = 0;
                d_events = 0;
                d_rev = 0;
            }
            let mut rev = 0i64;
            let mut purchasing = false;
            for x in &evs[s..e] {
                if x.kind == 3 {
                    purchasing = true;
                    rev += x.price as i64;
                    let c = x.cat as usize;
                    if acc.cat_rev[c] == 0 {
                        acc.touched.push(x.cat);
                    }
                    acc.cat_rev[c] += x.price as i64;
                }
            }
            d_sessions += 1;
            d_events += (e - s) as u32;
            d_rev += rev;
            // clicks window [start - 7d, start]
            while hi < cl.len() && cl[hi].ts <= start {
                let c = cl[hi].camp as usize;
                if acc.cnt[c] == 0 {
                    acc.posn[c] = acc.active.len() as u32;
                    acc.active.push(c as u32);
                }
                acc.cnt[c] += 1;
                hi += 1;
            }
            let floor = start as i64 - WEEK;
            while lo < hi && (cl[lo].ts as i64) < floor {
                let c = cl[lo].camp as usize;
                acc.cnt[c] -= 1;
                if acc.cnt[c] == 0 {
                    let at = acc.posn[c] as usize;
                    let last = acc.active.pop().unwrap();
                    if last as usize != c {
                        acc.active[at] = last;
                        acc.posn[last as usize] = at as u32;
                    }
                }
                lo += 1;
            }
            let credited = if hi > lo {
                let lt = cl[hi - 1].ts;
                let mut best = hi - 1;
                let mut k = hi - 1;
                while k > lo && cl[k - 1].ts == lt {
                    k -= 1;
                    if cmp_key(cl[k].ck, cl[best].ck) == Ordering::Less {
                        best = k;
                    }
                }
                cl[best].camp as usize
            } else {
                organic
            };
            let a = &mut acc.camp[credited * nd + di];
            a[0] += 1;
            a[1] += purchasing as i64;
            a[2] += rev;
            for &c in &acc.active {
                acc.camp[c as usize * nd + di][3] += 1;
            }
            s = e;
        }
        if d_sessions > 0 {
            let r = flush_day(person, cur_day, d_sessions, d_events, d_rev, &mut acc.cat_rev, &mut acc.touched);
            acc.rows.push(r);
        }
        for &c in &acc.active {
            acc.cnt[c as usize] = 0;
        }
        acc.active.clear();
        i = j;
    }
}

#[inline]
fn flush_day(person: u64, day: i64, sessions: u32, events: u32, revenue: i64, cat_rev: &mut [i64], touched: &mut Vec<u16>) -> UdRow {
    let mut top = -1i32;
    let mut best = 0i64;
    for &c in touched.iter() {
        let r = cat_rev[c as usize];
        if r > best || (r == best && (c as i32) < top) {
            best = r;
            top = c as i32;
        }
        cat_rev[c as usize] = 0;
    }
    touched.clear();
    UdRow { person, day: day as i32, sessions, events, revenue, top }
}

fn put_str(v: &mut Vec<u8>, s: &[u8]) {
    v.extend_from_slice(&(s.len() as u32).to_le_bytes());
    v.extend_from_slice(s);
}

fn write_user_daily(path: &Path, rows: &[UdRow], cat_names: &[Vec<u8>]) {
    let f = std::fs::File::create(path).expect("create user_daily");
    let mut w = parquet::Writer::new(std::io::BufWriter::with_capacity(1 << 22, f), parquet::USER_DAILY).unwrap();
    let mut tmp = Vec::with_capacity(32);
    for group in rows.chunks(1 << 20) {
        let n = group.len();
        let mut uid = Vec::with_capacity(n * 14);
        let mut day = Vec::with_capacity(n * 4);
        let mut ses = Vec::with_capacity(n * 4);
        let mut evs = Vec::with_capacity(n * 4);
        let mut rev = Vec::with_capacity(n * 8);
        let mut top = Vec::with_capacity(n * 4);
        let mut defs = vec![0u8; (n + 7) / 8 + 8];
        for (i, r) in group.iter().enumerate() {
            tmp.clear();
            key_bytes(r.person, &mut tmp);
            put_str(&mut uid, &tmp);
            day.extend_from_slice(&r.day.to_le_bytes());
            ses.extend_from_slice(&(r.sessions as i32).to_le_bytes());
            evs.extend_from_slice(&(r.events as i32).to_le_bytes());
            rev.extend_from_slice(&r.revenue.to_le_bytes());
            if r.top >= 0 {
                defs[i / 8] |= 1 << (i % 8);
                put_str(&mut top, &cat_names[r.top as usize]);
            }
        }
        w.row_group(
            n,
            &[
                parquet::ColData { values: &uid, defs: None },
                parquet::ColData { values: &day, defs: None },
                parquet::ColData { values: &ses, defs: None },
                parquet::ColData { values: &evs, defs: None },
                parquet::ColData { values: &rev, defs: None },
                parquet::ColData { values: &top, defs: Some(&defs) },
            ],
        )
        .unwrap();
    }
    w.finish().unwrap();
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
