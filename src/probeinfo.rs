//! Diagnostic rows: facts about the machine, as strings.
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

extern "C" {
    fn mincore(addr: *mut u8, len: usize, vec: *mut u8) -> i32;
}

fn one(path: &str) -> String {
    fs::read_to_string(path).map(|s| s.trim().replace('\n', " ")).unwrap_or_else(|_| "ERR".into())
}

fn bracket(s: &str) -> String {
    match (s.find('['), s.find(']')) {
        (Some(a), Some(b)) if b > a => s[a + 1..b].to_string(),
        _ => s.chars().take(12).collect(),
    }
}

pub fn start_facts() -> (f64, String) {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
    let age = |p: &str| fs::metadata(p).map(|m| now - (m.mtime() as f64 + m.mtime_nsec() as f64 * 1e-9)).unwrap_or(-1.0);
    (now, format!("tmp{:.3} proc1_{:.3} out{:.3} scr{:.3}", age("/tmp"), age("/proc/1"), age("/output"), age("/scratch")))
}

pub fn residency(maps: &[crate::sys::Mapped]) -> String {
    let (mut res, mut pages) = (0u64, 0u64);
    for m in maps {
        if m.len == 0 {
            continue;
        }
        let n = (m.len + 4095) / 4096;
        let mut v = vec![0u8; n];
        if unsafe { mincore(m.ptr as *mut u8, m.len, v.as_mut_ptr()) } != 0 {
            return "mincoreERR".into();
        }
        pages += n as u64;
        res += v.iter().filter(|&&b| b & 1 == 1).count() as u64;
    }
    format!("cached{res}/{pages}")
}

pub fn rows(start: &str, resid: &str, phases: &str) -> Vec<String> {
    let cpu = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let get = |k: &str| cpu.lines().find(|l| l.starts_with(k)).map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string()).unwrap_or_default();
    let flags = get("flags");
    let has = |f: &str| if flags.split(' ').any(|x| x == f) { "1" } else { "0" };
    let mut v = Vec::new();
    v.push(format!(
        "!A {} | {} MHz | n={} | L2={} L3={} sib={} | {}",
        get("model name"),
        get("cpu MHz"),
        cpu.matches("processor\t").count(),
        one("/sys/devices/system/cpu/cpu0/cache/index2/size"),
        one("/sys/devices/system/cpu/cpu0/cache/index3/size"),
        one("/sys/devices/system/cpu/cpu0/topology/thread_siblings_list"),
        one("/proc/version").chars().take(40).collect::<String>()
    ));
    v.push(format!(
        "!B avx512f{} bw{} vbmi{} vbmi2{} gfni{} bmi2{} | thp={} defrag={} shmem={} | oc={} cpus={} mem={} load={}",
        has("avx512f"),
        has("avx512bw"),
        has("avx512vbmi"),
        has("avx512_vbmi2"),
        has("gfni"),
        has("bmi2"),
        bracket(&one("/sys/kernel/mm/transparent_hugepage/enabled")),
        bracket(&one("/sys/kernel/mm/transparent_hugepage/defrag")),
        bracket(&one("/sys/kernel/mm/transparent_hugepage/shmem_enabled")),
        one("/proc/sys/vm/overcommit_memory"),
        one("/sys/fs/cgroup/cpuset.cpus.effective"),
        one("/sys/fs/cgroup/memory.max"),
        one("/proc/loadavg").chars().take(14).collect::<String>()
    ));
    let mut mounts = String::new();
    if let Ok(s) = fs::read_to_string("/proc/mounts") {
        for l in s.lines() {
            let f: Vec<&str> = l.split(' ').collect();
            if f.len() > 3 && ["/data", "/output", "/scratch", "/"].contains(&f[1]) {
                mounts.push_str(&format!("{}:{}:{} ; ", f[1], f[2], f[3].chars().take(28).collect::<String>()));
            }
        }
    }
    v.push(format!("!C {mounts}"));
    // first-touch cost of anonymous memory, with and without the huge page hint
    let mut touch = String::new();
    for huge in [false, true] {
        let len = 256usize << 20;
        let t = Instant::now();
        let p = crate::sys::anon(len, huge);
        let mut i = 0;
        while i < len {
            unsafe { *p.add(i) = 1 };
            i += 4096;
        }
        let dt = t.elapsed().as_secs_f64() * 1e3;
        let ahp = fs::read_to_string("/proc/self/smaps_rollup").ok().and_then(|s| s.lines().find(|l| l.starts_with("AnonHugePages")).map(|l| l.split_whitespace().nth(1).unwrap_or("?").to_string())).unwrap_or_default();
        touch.push_str(&format!("touch256M huge={} {:.1}ms AnonHuge={}kB ; ", huge as u8, dt, ahp));
    }
    let wt = {
        let t = Instant::now();
        let buf = vec![7u8; 32 << 20];
        let out = std::env::var("OUTPUT_DIR").unwrap_or("/output".into());
        let _ = fs::write(format!("{out}/_probe.bin"), &buf);
        let dt = t.elapsed().as_secs_f64() * 1e3;
        let _ = fs::remove_file(format!("{out}/_probe.bin"));
        dt
    };
    v.push(format!("!D start[{start}] {resid} | {touch}write32M {wt:.1}ms"));
    v.push(format!("!E {phases}"));
    v
}
