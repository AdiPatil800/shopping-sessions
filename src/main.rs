//! Shopping Sessions pipeline: one static binary, no crates.
//!
//! Phases:
//!  1. lookups (manifest, products, campaigns) and links -> union-find -> person names
//!  2. parse events and clicks in parallel. Events go to buckets by event_id hash,
//!     with every per-copy validation done already; clicks are validated, mapped to
//!     their person and go to partitions by person hash.
//!  3. per event_id bucket: keep the copy with the smallest ingest_ts; if it is
//!     valid, map it to its person and send it to the person's partition.
//!  4. per person partition: sort, sessionize, credit and assist, aggregate.
//!  5. write the two Parquet tables.

mod ids;
mod parquet;
mod sys;
mod ts;

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

/// Event-id buckets (phase 1 scatter) and person partitions (phase 2 scatter).
const BBITS: u32 = 10;
const NB: usize = 1 << BBITS;
const PBITS: u32 = 10;
const NP: usize = 1 << PBITS;

#[inline(always)]
fn top_bits(k: u64, bits: u32) -> usize {
    (k.wrapping_mul(K1) >> (64 - bits)) as usize
}

// ---------------------------------------------------------------------------
// Small open-addressing map u64 -> u64 (key 0 = empty; IDs never map to 0).

struct U64Map {
    keys: Vec<u64>,
    vals: Vec<u64>,
    shift: u32,
    mask: usize,
}

impl U64Map {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { keys: vec![0; cap], vals: vec![0; cap], shift: 64 - cap.trailing_zeros(), mask: cap - 1 }
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (k.wrapping_mul(K2) >> self.shift) as usize
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u64> {
        let mut i = self.slot(k);
        loop {
            let x = unsafe { *self.keys.get_unchecked(i) };
            if x == k {
                return Some(unsafe { *self.vals.get_unchecked(i) });
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
            let x = self.keys[i];
            if x == k {
                return self.vals[i];
            }
            if x == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                return v;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------------------
// CSV dialect.

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

/// Splits one line (no `\n`) into exactly N fields. Returns false for a
/// malformed line. Each field is (offset, len) of its value inside `line`.
#[inline(always)]
fn split<const N: usize>(line: &[u8], out: &mut [(u32, u32); N]) -> bool {
    let len = line.len();
    let mut n = 0usize;
    let mut start = 0usize;
    loop {
        let mut j = start;
        let mut quotes = 0u32;
        while j < len {
            let c = unsafe { *line.get_unchecked(j) };
            if c == b',' {
                break;
            }
            quotes += (c == b'"') as u32;
            j += 1;
        }
        if n == N {
            return false;
        }
        if quotes == 0 {
            out[n] = (start as u32, (j - start) as u32);
        } else if quotes == 2 && j - start >= 2 && line[start] == b'"' && line[j - 1] == b'"' {
            out[n] = (start as u32 + 1, (j - start - 2) as u32);
        } else {
            return false;
        }
        n += 1;
        if j >= len {
            break;
        }
        start = j + 1;
    }
    n == N
}

#[inline(always)]
fn field<'a>(line: &'a [u8], f: (u32, u32)) -> &'a [u8] {
    unsafe { line.get_unchecked(f.0 as usize..(f.0 + f.1) as usize) }
}

/// Key of a non-empty field; `fend` is the end of the mapped file.
#[inline(always)]
fn key_of(line: &[u8], f: (u32, u32), fend: usize) -> u64 {
    let p = unsafe { line.as_ptr().add(f.0 as usize) };
    let len = f.1 as usize;
    if len <= 9 && fend - (p as usize) >= 8 {
        unsafe { id_key(p, len) }
    } else {
        id_key_slice(field(line, f))
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

/// Lines of a small CSV file after its header.
fn small_csv<const N: usize>(path: &Path) -> Vec<[Vec<u8>; N]> {
    let bytes = std::fs::read(path).unwrap_or_default();
    let mut rows = Vec::new();
    let mut pos = match bytes.iter().position(|&c| c == b'\n') {
        Some(i) => i + 1,
        None => return rows,
    };
    let mut f = [(0u32, 0u32); N];
    while pos < bytes.len() {
        let nl = find_nl(&bytes, pos);
        let line = &bytes[pos..nl];
        if split::<N>(line, &mut f) {
            rows.push(std::array::from_fn(|i| field(line, f[i]).to_vec()));
        }
        pos = nl + 1;
    }
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

// ---------------------------------------------------------------------------
// Records.

/// One parsed event copy, before dedup. kind 0 = the copy is invalid.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    ek: u64,
    uk: u64,
    ingest: i64,
    ts: u32,
    price: u32,
    cat: u16,
    kind: u8,
    _p: u8,
}

/// A surviving event in its person partition.
#[derive(Clone, Copy)]
#[repr(C)]
struct E2 {
    person: u64,
    ts: u32,
    price: u32,
    cat: u16,
    kind: u8,
    _p: u8,
}

/// A valid click in its person partition.
#[derive(Clone, Copy)]
#[repr(C)]
struct C2 {
    person: u64,
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
    let pairs: Vec<Vec<(u64, u64)>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut v = Vec::new();
                    let mut f = [(0u32, 0u32); 2];
                    loop {
                        let k = next.fetch_add(1, Relaxed);
                        let Some(c) = lchunks.get(k) else { break };
                        let m = &maps[c.file as usize];
                        let b = m.bytes();
                        let fend = b.as_ptr() as usize + b.len();
                        let mut pos = c.start;
                        while pos < c.end {
                            let nl = find_nl(&b[..c.end], pos);
                            let line = &b[pos..nl];
                            pos = nl + 1;
                            if !split::<2>(line, &mut f) || f[0].1 == 0 || f[1].1 == 0 {
                                continue;
                            }
                            v.push((key_of(line, f[0], fend), key_of(line, f[1], fend)));
                        }
                    }
                    v
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
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
        for &(a, b) in v {
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
                // smaller index becomes the root (keeps things deterministic)
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
    make_chunks(&maps[..nev], 0, 0, 2 << 20, &mut chunks);
    make_chunks(&maps[nev..nev + nck], nev, 1, 2 << 20, &mut chunks);
    // big chunks first: events before clicks is fine, all chunks are ~equal
    let est_events = ev_bytes / 60 + 1024;
    let est_clicks = ck_bytes / 40 + 1024;
    let per_b = est_events / (nthreads * NB) + 16;
    let per_c = est_clicks / (nthreads * NP) + 16;
    let next = AtomicUsize::new(0);
    let parsed: Vec<(Vec<Vec<Ev>>, Vec<Vec<C2>>)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                let ctx = &ctx;
                let chunks = &chunks;
                let maps = &maps;
                let next = &next;
                s.spawn(move || {
                    let mut eb: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::with_capacity(per_b)).collect();
                    let mut cb: Vec<Vec<C2>> = (0..NP).map(|_| Vec::with_capacity(per_c)).collect();
                    loop {
                        let k = next.fetch_add(1, Relaxed);
                        let Some(c) = chunks.get(k) else { break };
                        let b = maps[c.file as usize].bytes();
                        if c.kind == 0 {
                            parse_events(ctx, b, c.start, c.end, &mut eb);
                        } else {
                            parse_clicks(ctx, b, c.start, c.end, &mut cb);
                        }
                    }
                    (eb, cb)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let (ebufs, cbufs): (Vec<_>, Vec<_>) = parsed.into_iter().unzip();
    let n_ev: usize = ebufs.iter().map(|t| t.iter().map(|v| v.len()).sum::<usize>()).sum();
    let n_ck: usize = cbufs.iter().map(|t| t.iter().map(|v| v.len()).sum::<usize>()).sum();
    log(&t0, &format!("parsed: {n_ev} event copies, {n_ck} valid clicks"));

    // --- dedup events per bucket, send survivors to person partitions
    let next = AtomicUsize::new(0);
    let per_p = n_ev / (nthreads * NP) + 16;
    let deduped: Vec<Vec<Vec<E2>>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                let ctx = &ctx;
                let ebufs = &ebufs;
                let next = &next;
                s.spawn(move || {
                    let mut outp: Vec<Vec<E2>> = (0..NP).map(|_| Vec::with_capacity(per_p)).collect();
                    let mut table: Vec<(u64, *const Ev)> = Vec::new();
                    loop {
                        let bkt = next.fetch_add(1, Relaxed);
                        if bkt >= NB {
                            break;
                        }
                        let n: usize = ebufs.iter().map(|t| t[bkt].len()).sum();
                        if n == 0 {
                            continue;
                        }
                        let cap = (n * 2).next_power_of_two().max(16);
                        let shift = 64 - cap.trailing_zeros();
                        let mask = cap - 1;
                        table.clear();
                        table.resize(cap, (0, std::ptr::null()));
                        for t in ebufs.iter() {
                            for e in t[bkt].iter() {
                                let mut i = (e.ek.wrapping_mul(K2) >> shift) as usize;
                                loop {
                                    let slot = unsafe { table.get_unchecked_mut(i) };
                                    if slot.0 == 0 {
                                        *slot = (e.ek, e as *const Ev);
                                        break;
                                    }
                                    if slot.0 == e.ek {
                                        if e.ingest < unsafe { (*slot.1).ingest } {
                                            slot.1 = e as *const Ev;
                                        }
                                        break;
                                    }
                                    i = (i + 1) & mask;
                                }
                            }
                        }
                        for &(k, p) in table.iter() {
                            if k == 0 {
                                continue;
                            }
                            let e = unsafe { &*p };
                            if e.kind == 0 {
                                continue;
                            }
                            let person = ctx.person.get(e.uk).unwrap_or(e.uk);
                            outp[top_bits(person, PBITS)].push(E2 { person, ts: e.ts, price: e.price, cat: e.cat, kind: e.kind, _p: 0 });
                        }
                    }
                    outp
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let n_surv: usize = deduped.iter().map(|t| t.iter().map(|v| v.len()).sum::<usize>()).sum();
    log(&t0, &format!("dedup: {n_surv} surviving events"));

    // --- sessionize per person partition
    let next = AtomicUsize::new(0);
    let results: Vec<(Vec<UdRow>, Vec<[i64; 4]>)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                let ctx = &ctx;
                let deduped = &deduped;
                let cbufs = &cbufs;
                let next = &next;
                s.spawn(move || sessionize_worker(ctx, deduped, cbufs, next))
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
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
        let mut any = false;
        for (t, (rows, _)) in results.iter().enumerate() {
            if rows.is_empty() && (any || t + 1 < results.len() || total_ud > 0) {
                continue;
            }
            any = true;
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
fn parse_events(ctx: &Ctx, b: &[u8], start: usize, end: usize, eb: &mut [Vec<Ev>]) {
    let fend = b.as_ptr() as usize + b.len();
    let b = &b[..end];
    let mut f = [(0u32, 0u32); 7];
    let mut pos = start;
    while pos < end {
        let nl = find_nl(b, pos);
        let line = unsafe { b.get_unchecked(pos..nl) };
        pos = nl + 1;
        if !split::<7>(line, &mut f) || f[0].1 == 0 {
            continue;
        }
        let ek = key_of(line, f[0], fend);
        let ingest = ts::parse_ts_slice(field(line, f[3])).unwrap_or(i64::MAX);
        let mut e = Ev { ek, uk: 0, ingest, ts: 0, price: 0, cat: 0, kind: 0, _p: 0 };
        'v: {
            if f[1].1 == 0 || f[5].1 == 0 {
                break 'v;
            }
            let Some(t) = ts::parse_ts_slice(field(line, f[2])) else { break 'v };
            let rel = t - ctx.base;
            if rel < ctx.ws_rel as i64 || rel >= ctx.we_rel as i64 {
                break 'v;
            }
            let kind = match field(line, f[4]) {
                b"view" => 1u8,
                b"cart" => 2,
                b"purchase" => 3,
                _ => break 'v,
            };
            if kind == 3 {
                let Some(p) = parse_price(field(line, f[6])) else { break 'v };
                e.price = p;
            }
            if ingest.saturating_sub(t) > DAY {
                break 'v;
            }
            if kind == 3 {
                let pk = key_of(line, f[5], fend);
                e.cat = ctx.prod.get(pk).map(|c| c as u16).unwrap_or(ctx.unknown_cat);
            }
            e.ts = rel as u32;
            e.uk = key_of(line, f[1], fend);
            e.kind = kind;
        }
        unsafe { eb.get_unchecked_mut(top_bits(ek, BBITS)) }.push(e);
    }
}

#[inline(always)]
fn parse_clicks(ctx: &Ctx, b: &[u8], start: usize, end: usize, cb: &mut [Vec<C2>]) {
    let fend = b.as_ptr() as usize + b.len();
    let b = &b[..end];
    let mut f = [(0u32, 0u32); 4];
    let mut pos = start;
    while pos < end {
        let nl = find_nl(b, pos);
        let line = unsafe { b.get_unchecked(pos..nl) };
        pos = nl + 1;
        if !split::<4>(line, &mut f) || f[0].1 == 0 || f[1].1 == 0 || f[2].1 == 0 {
            continue;
        }
        let Some(t) = ts::parse_ts_slice(field(line, f[3])) else { continue };
        let rel = t - ctx.base;
        if rel < 0 || rel >= ctx.we_rel as i64 {
            continue;
        }
        let Some(camp) = ctx.camp.get(key_of(line, f[2], fend)) else { continue };
        let uk = key_of(line, f[1], fend);
        let person = ctx.person.get(uk).unwrap_or(uk);
        let ck = key_of(line, f[0], fend);
        unsafe { cb.get_unchecked_mut(top_bits(person, PBITS)) }.push(C2 { person, ck, ts: rel as u32, camp: camp as u32 });
    }
}

fn sessionize_worker(ctx: &Ctx, deduped: &[Vec<Vec<E2>>], cbufs: &[Vec<Vec<C2>>], next: &AtomicUsize) -> (Vec<UdRow>, Vec<[i64; 4]>) {
    let nd = ctx.ndays;
    let mut camp_acc = vec![[0i64; 4]; (ctx.ncamp + 1) * nd]; // sessions, purchasing, revenue, assisted
    let mut rows: Vec<UdRow> = Vec::new();
    let mut evs: Vec<E2> = Vec::new();
    let mut cks: Vec<C2> = Vec::new();
    let mut cnt = vec![0u32; ctx.ncamp];
    let mut posn = vec![0u32; ctx.ncamp];
    let mut active: Vec<u32> = Vec::new();
    let mut cat_rev = vec![0i64; ctx.ncats];
    let mut touched: Vec<u16> = Vec::new();
    let organic = ctx.ncamp;
    loop {
        let p = next.fetch_add(1, Relaxed);
        if p >= NP {
            break;
        }
        evs.clear();
        for t in deduped {
            evs.extend_from_slice(&t[p]);
        }
        cks.clear();
        for t in cbufs {
            cks.extend_from_slice(&t[p]);
        }
        evs.sort_unstable_by_key(|e| ((e.person as u128) << 32) | e.ts as u128);
        cks.sort_unstable_by_key(|c| ((c.person as u128) << 32) | c.ts as u128);
        let mut ci = 0usize;
        let mut i = 0usize;
        while i < evs.len() {
            let person = evs[i].person;
            let mut j = i;
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
            // person-day accumulator
            let mut cur_day = i64::MIN;
            let (mut d_sessions, mut d_events, mut d_rev) = (0u32, 0u32, 0i64);
            let mut s = i;
            while s < j {
                let start = evs[s].ts;
                let mut e = s + 1;
                while e < j && evs[e].ts - evs[e - 1].ts <= GAP {
                    e += 1;
                }
                // session evs[s..e]
                let day = (ctx.base + start as i64).div_euclid(DAY);
                let di = (day - ctx.first_day) as usize;
                if day != cur_day {
                    if d_sessions > 0 {
                        rows.push(flush_day(person, cur_day, d_sessions, d_events, d_rev, &mut cat_rev, &mut touched));
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
                        if cat_rev[c] == 0 {
                            touched.push(x.cat);
                        }
                        cat_rev[c] += x.price as i64;
                    }
                }
                d_sessions += 1;
                d_events += (e - s) as u32;
                d_rev += rev;
                // clicks window [start - 7d, start]
                while hi < cl.len() && cl[hi].ts <= start {
                    let c = cl[hi].camp as usize;
                    if cnt[c] == 0 {
                        posn[c] = active.len() as u32;
                        active.push(c as u32);
                    }
                    cnt[c] += 1;
                    hi += 1;
                }
                let floor = start as i64 - WEEK;
                while lo < hi && (cl[lo].ts as i64) < floor {
                    let c = cl[lo].camp as usize;
                    cnt[c] -= 1;
                    if cnt[c] == 0 {
                        let at = posn[c] as usize;
                        let last = active.pop().unwrap();
                        if last as usize != c {
                            active[at] = last;
                            posn[last as usize] = at as u32;
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
                let a = &mut camp_acc[credited * nd + di];
                a[0] += 1;
                a[1] += purchasing as i64;
                a[2] += rev;
                for &c in &active {
                    camp_acc[c as usize * nd + di][3] += 1;
                }
                s = e;
            }
            if d_sessions > 0 {
                rows.push(flush_day(person, cur_day, d_sessions, d_events, d_rev, &mut cat_rev, &mut touched));
            }
            for &c in &active {
                cnt[c as usize] = 0;
            }
            active.clear();
            i = j;
        }
    }
    (rows, camp_acc)
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
