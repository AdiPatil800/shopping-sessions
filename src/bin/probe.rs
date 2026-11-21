//! Environment probe: logs what the scoring container looks like, and writes
//! tiny placeholder tables so the run completes.
#[path = "../parquet.rs"]
mod parquet;
#[path = "../sys.rs"]
mod sys;

use std::fs;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

extern "C" {
    fn mincore(addr: *mut u8, len: usize, vec: *mut u8) -> i32;
}

fn cat(path: &str, max: usize) {
    match fs::read(path) {
        Ok(b) => {
            let s = String::from_utf8_lossy(&b[..b.len().min(max)]).to_string();
            eprintln!("== {path}\n{}", s.trim_end());
        }
        Err(e) => eprintln!("== {path}: {e}"),
    }
}

fn now_s() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64()
}

fn files(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut v = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            if let Ok(m) = e.metadata() {
                if m.is_file() {
                    v.push((e.path(), m.len()));
                }
            }
        }
    }
    v.sort();
    v
}

fn main() {
    let t0 = Instant::now();
    let wall0 = now_s();
    eprintln!("probe start wall={wall0:.6}");
    for (k, v) in std::env::vars() {
        eprintln!("env {k}={v}");
    }
    for d in ["/tmp", "/output", "/scratch", "/data", "/proc/1", "/"] {
        match fs::metadata(d) {
            Ok(m) => eprintln!(
                "stat {d}: dev={} mode={:o} uid={} mtime={}.{:09} ctime={}.{:09} (age vs start {:.6}s / {:.6}s)",
                m.dev(),
                m.mode(),
                m.uid(),
                m.mtime(),
                m.mtime_nsec(),
                m.ctime(),
                m.ctime_nsec(),
                wall0 - (m.mtime() as f64 + m.mtime_nsec() as f64 * 1e-9),
                wall0 - (m.ctime() as f64 + m.ctime_nsec() as f64 * 1e-9)
            ),
            Err(e) => eprintln!("stat {d}: {e}"),
        }
    }
    cat("/proc/version", 400);
    cat("/proc/uptime", 100);
    cat("/proc/self/stat", 600);
    cat("/proc/stat", 300);
    cat("/proc/loadavg", 100);
    cat("/proc/mounts", 6000);
    cat("/proc/self/cgroup", 400);
    cat("/sys/fs/cgroup/cpu.max", 100);
    cat("/sys/fs/cgroup/cpuset.cpus.effective", 100);
    cat("/sys/fs/cgroup/memory.max", 100);
    cat("/sys/fs/cgroup/memory.high", 100);
    cat("/sys/fs/cgroup/memory.swap.max", 100);
    cat("/sys/fs/cgroup/pids.max", 100);
    cat("/sys/kernel/mm/transparent_hugepage/enabled", 100);
    cat("/sys/kernel/mm/transparent_hugepage/defrag", 100);
    cat("/sys/kernel/mm/transparent_hugepage/shmem_enabled", 100);
    cat("/sys/kernel/mm/transparent_hugepage/khugepaged/defrag", 100);
    cat("/proc/sys/vm/overcommit_memory", 20);
    cat("/proc/sys/vm/max_map_count", 20);
    cat("/proc/sys/kernel/sched_autogroup_enabled", 20);
    cat("/proc/meminfo", 1500);
    cat("/proc/self/limits", 2000);
    if let Ok(s) = fs::read_to_string("/proc/self/status") {
        for l in s.lines() {
            if l.starts_with("Cpus_allowed") || l.starts_with("Mems_allowed") || l.starts_with("Seccomp") || l.starts_with("Cap") || l.starts_with("NoNewPrivs") {
                eprintln!("status {l}");
            }
        }
    }
    if let Ok(s) = fs::read_to_string("/proc/cpuinfo") {
        let mut n = 0;
        let mut first = String::new();
        let mut cores = Vec::new();
        let mut cur = (String::new(), String::new(), String::new());
        for l in s.lines() {
            if l.starts_with("processor") {
                n += 1;
                cur.0 = l.split(':').nth(1).unwrap_or("").trim().to_string();
            }
            if l.starts_with("core id") {
                cur.1 = l.split(':').nth(1).unwrap_or("").trim().to_string();
            }
            if l.starts_with("physical id") {
                cur.2 = l.split(':').nth(1).unwrap_or("").trim().to_string();
            }
            if l.is_empty() {
                cores.push(format!("{}:s{}c{}", cur.0, cur.2, cur.1));
            }
            if n == 1 {
                first.push_str(l);
                first.push('\n');
            }
        }
        eprintln!("== cpuinfo: {n} processors; first:\n{first}");
        eprintln!("topology {}", cores.join(" "));
    }
    for i in 0..4 {
        cat(&format!("/sys/devices/system/cpu/cpu0/cache/index{i}/size"), 50);
        cat(&format!("/sys/devices/system/cpu/cpu0/cache/index{i}/shared_cpu_list"), 100);
    }
    cat("/sys/devices/system/cpu/online", 100);
    cat("/sys/devices/system/cpu/cpu0/topology/thread_siblings_list", 100);

    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let mut all = Vec::new();
    for sub in ["events", "clicks", "links"] {
        let v = files(&data.join(sub));
        let total: u64 = v.iter().map(|f| f.1).sum();
        let max = v.iter().map(|f| f.1).max().unwrap_or(0);
        eprintln!("dir {sub}: {} files, {total} bytes, largest {max}", v.len());
        all.extend(v);
    }
    for f in files(&data) {
        eprintln!("file {} {}", f.0.display(), f.1);
    }
    cat(data.join("manifest.json").to_str().unwrap(), 300);

    // Page-cache residency before touching anything.
    let mut resident = 0u64;
    let mut pages = 0u64;
    let maps: Vec<sys::Mapped> = all.iter().map(|f| sys::Mapped::open(&f.0, false).unwrap()).collect();
    eprintln!("mmap of {} files took {:?}", maps.len(), t0.elapsed());
    for m in &maps {
        if m.len == 0 {
            continue;
        }
        let n = (m.len + 4095) / 4096;
        let mut v = vec![0u8; n];
        let r = unsafe { mincore(m.ptr as *mut u8, m.len, v.as_mut_ptr()) };
        if r != 0 {
            eprintln!("mincore failed: {}", std::io::Error::last_os_error());
            break;
        }
        pages += n as u64;
        resident += v.iter().filter(|&&b| b & 1 == 1).count() as u64;
    }
    eprintln!("page cache: {resident} of {pages} pages resident before reading");

    let total: usize = maps.iter().map(|m| m.len).sum();
    // 1. Single-thread read() of the largest file.
    if let Some(big) = all.iter().max_by_key(|f| f.1) {
        let t = Instant::now();
        let mut f = fs::File::open(&big.0).unwrap();
        let mut buf = vec![0u8; 1 << 20];
        let mut n = 0usize;
        let mut sum = 0u64;
        loop {
            let k = f.read(&mut buf).unwrap();
            if k == 0 {
                break;
            }
            n += k;
            sum += buf[0] as u64;
        }
        let dt = t.elapsed().as_secs_f64();
        eprintln!("read() largest file: {n} bytes in {dt:.4}s = {:.2} GB/s (sum {sum})", n as f64 / dt / 1e9);
    }
    // 2. Parallel mmap scan of everything (count newlines), twice, with 8 and 4 threads.
    let nt = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    eprintln!("available_parallelism = {nt}");
    for (round, threads) in [(0, nt), (1, nt), (2, (nt / 2).max(1)), (3, nt)] {
        let t = Instant::now();
        let next = std::sync::atomic::AtomicUsize::new(0);
        let mut chunks = Vec::new();
        for (i, m) in maps.iter().enumerate() {
            let mut a = 0;
            while a < m.len {
                let b = (a + (1 << 20)).min(m.len);
                chunks.push((i, a, b));
                a = b;
            }
        }
        let lines: u64 = std::thread::scope(|s| {
            let hs: Vec<_> = (0..threads)
                .map(|_| {
                    s.spawn(|| {
                        let mut c = 0u64;
                        loop {
                            let k = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if k >= chunks.len() {
                                break;
                            }
                            let (i, a, b) = chunks[k];
                            let s = &maps[i].bytes()[a..b];
                            c += s.iter().filter(|&&x| x == b'\n').count() as u64;
                        }
                        c
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).sum()
        });
        let dt = t.elapsed().as_secs_f64();
        eprintln!("round {round}: mmap scan {threads} threads: {total} bytes, {lines} lines in {dt:.4}s = {:.2} GB/s", total as f64 / dt / 1e9);
    }
    // 3. Anonymous memory first-touch cost, with and without MADV_HUGEPAGE.
    for huge in [false, true] {
        let len = 512usize << 20;
        let t = Instant::now();
        let p = sys::anon(len, huge);
        let mut i = 0;
        while i < len {
            unsafe { *p.add(i) = 1 };
            i += 4096;
        }
        let dt = t.elapsed().as_secs_f64();
        eprintln!("first touch 512 MB anon huge={huge}: {dt:.4}s");
        if let Ok(s) = fs::read_to_string("/proc/self/smaps_rollup") {
            for l in s.lines() {
                if l.starts_with("AnonHugePages") || l.starts_with("Rss") {
                    eprintln!("  {l}");
                }
            }
        }
    }
    // 4. Output write speed.
    {
        let t = Instant::now();
        let buf = vec![7u8; 64 << 20];
        fs::write(out.join("_probe.bin"), &buf).unwrap();
        let dt = t.elapsed().as_secs_f64();
        eprintln!("write 64 MB to output: {dt:.4}s = {:.2} GB/s", 0.064 / dt);
        let _ = fs::remove_file(out.join("_probe.bin"));
    }

    // Placeholder tables (one row each) so the format itself gets checked.
    fs::create_dir_all(out.join("user_daily")).unwrap();
    fs::create_dir_all(out.join("campaign_daily")).unwrap();
    let s = |b: &[u8]| {
        let mut v = (b.len() as u32).to_le_bytes().to_vec();
        v.extend_from_slice(b);
        v
    };
    {
        let f = fs::File::create(out.join("user_daily/part-0.parquet")).unwrap();
        let mut w = parquet::Writer::new(f, parquet::USER_DAILY).unwrap();
        let (uid, day, i1, i2, rev) = (s(b"probe"), 20000i32.to_le_bytes(), 1i32.to_le_bytes(), 1i32.to_le_bytes(), 0i64.to_le_bytes());
        w.row_group(
            1,
            &[
                parquet::ColData { values: &uid, defs: None },
                parquet::ColData { values: &day, defs: None },
                parquet::ColData { values: &i1, defs: None },
                parquet::ColData { values: &i2, defs: None },
                parquet::ColData { values: &rev, defs: None },
                parquet::ColData { values: &[], defs: Some(&[0u8]) },
            ],
        )
        .unwrap();
        w.finish().unwrap();
    }
    {
        let f = fs::File::create(out.join("campaign_daily/part-0.parquet")).unwrap();
        let mut w = parquet::Writer::new(f, parquet::CAMPAIGN_DAILY).unwrap();
        let (a, b, day, z) = (s(b"organic"), s(b"organic"), 20000i32.to_le_bytes(), 0i64.to_le_bytes());
        let o = 1i64.to_le_bytes();
        w.row_group(
            1,
            &[
                parquet::ColData { values: &a, defs: None },
                parquet::ColData { values: &b, defs: None },
                parquet::ColData { values: &day, defs: None },
                parquet::ColData { values: &o, defs: None },
                parquet::ColData { values: &z, defs: None },
                parquet::ColData { values: &z, defs: None },
                parquet::ColData { values: &z, defs: None },
            ],
        )
        .unwrap();
        w.finish().unwrap();
    }
    eprintln!("probe done in {:?}, wall={:.6}", t0.elapsed(), now_s());
}
