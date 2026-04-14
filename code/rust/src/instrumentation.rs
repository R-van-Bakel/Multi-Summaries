use std::sync::Mutex;

pub fn collector() -> &'static Mutex<Vec<Stats>> {
    crate::instrumentation::internals::_COLLECTOR.get_or_init(|| Mutex::new(Vec::new()))
}

#[derive(Debug)]
pub struct Stats {
    pub uptime_secs_before: u64,
    pub user_cpu_secs_before: f64,
    pub system_cpu_secs_before: f64,
    pub uptime_secs_after: u64,
    pub user_cpu_secs_after: f64,
    pub system_cpu_secs_after: f64,
    pub current_rss_bytes_after: u64,
    pub peak_rss_bytes_after: u64,
}

pub mod internals {
    use crate::instrumentation::Stats;
    use procfs::process::Process;
    use procfs::{CurrentSI, KernelStats};
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub static _COLLECTOR: OnceLock<Mutex<Vec<Stats>>> = OnceLock::new();

    #[cfg(feature = "instrument")]
    pub fn _get_mem_stats() -> (u64, u64) {
        // Get the current process
        let me = Process::myself().expect("procfs could not get the current process");

        // --- Current RSS (resident set size) ---
        // statm.rss is in pages, so multiply by page size
        let statm = me
            .statm()
            .expect("procfs could not access statm for the current process");
        let page_size = procfs::page_size();
        let current_rss_bytes = statm.resident * page_size;

        // --- Peak RSS (maximum resident set size so far) ---
        // VmHWM is in kB
        let status = me
            .status()
            .expect("procfs could not access status for the current process");
        let peak_rss_bytes = status.vmhwm.unwrap_or(0) * 1024;

        (current_rss_bytes, peak_rss_bytes)
    }

    #[cfg(feature = "instrument")]
    pub fn _get_time_stats() -> (u64, f64, f64) {
        let me = Process::myself().unwrap();
        let stat = me.stat().unwrap();

        let kstats = KernelStats::current().expect("procfs: could not read kernel stats");
        let btime = kstats.btime;

        let ticks = procfs::ticks_per_second() as f64;

        let start_secs = stat.starttime as u64 / ticks as u64;
        let start_timestamp = btime + start_secs;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let uptime_secs = now - start_timestamp;

        let user_cpu_secs = stat.utime as f64 / ticks;
        let system_cpu_secs = stat.stime as f64 / ticks;

        (uptime_secs, user_cpu_secs, system_cpu_secs)
    }
}

// Enabled version
#[cfg(feature = "instrument")]
#[macro_export]
macro_rules! instrument {
    ($e:expr) => {{
        let (uptime_secs_before, user_cpu_secs_before, system_cpu_secs_before) =
            $crate::instrumentation::internals::_get_time_stats();

        let result = $e;

        let (uptime_secs_after, user_cpu_secs_after, system_cpu_secs_after) =
            $crate::instrumentation::internals::_get_time_stats();
        let (current_rss_bytes_after, peak_rss_bytes_after) =
            $crate::instrumentation::internals::_get_mem_stats();

        $crate::instrumentation::collector()
            .lock()
            .unwrap()
            .push($crate::instrumentation::Stats {
                uptime_secs_before,
                user_cpu_secs_before,
                system_cpu_secs_before,
                uptime_secs_after,
                user_cpu_secs_after,
                system_cpu_secs_after,
                current_rss_bytes_after,
                peak_rss_bytes_after,
            });

        result
    }};
}

// Disabled version
#[cfg(not(feature = "instrument"))]
#[macro_export]
macro_rules! instrument {
    ($e:expr) => {{ $e }};
}
