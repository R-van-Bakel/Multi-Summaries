use serde::{Deserialize, Serialize};
use std::{fs::File, path::Path};
use std::{ops::Sub, sync::Mutex};
use time::OffsetDateTime;

pub fn stats_collector() -> &'static Mutex<Vec<Stats>> {
    crate::instrumentation::internal::_COLLECTOR.get_or_init(|| Mutex::new(Vec::new()))
}

// Enabled version
#[cfg(feature = "instrument")]
pub fn print_format_last<S1, S2>(prepend_message: S1, append_message: S2)
where
    S1: AsRef<str>,
    S2: AsRef<str>,
{
    let collected_stats = stats_collector().lock().unwrap();
    let last_stats = collected_stats.last().unwrap();
    let TimeStats {
        uptime_secs,
        user_cpu_secs,
        system_cpu_secs,
    } = last_stats.durations();
    let MemStats {
        current_rss_bytes,
        peak_rss_bytes,
    } = last_stats.mem_after.clone();
    let now = OffsetDateTime::now_local().expect("time could not get the local time");
    let time_string = format!(
        "{} - Duration (seconds) --> Uptime: {:<15.2}, User: {:<14.2}, System: {:<10.2}",
        now, uptime_secs, user_cpu_secs, system_cpu_secs
    );
    let mem_string = format!(
        "{} - Memory after (MiB) --> Current RSS: {:<10.2}, Peak RSS: {:<10.2}",
        now,
        current_rss_bytes as f64 / 1024_u64.pow(2) as f64,
        peak_rss_bytes as f64 / 1024_u64.pow(2) as f64
    );
    println!(
        "{}Statistics for \"{}\":\n{}\n{}{}",
        prepend_message.as_ref(),
        last_stats.label,
        time_string,
        mem_string,
        append_message.as_ref(),
    );
}

// Disabled version
#[cfg(not(feature = "instrument"))]
pub fn print_format_last<S1, S2>(_prepend_message: S1, _append_message: S2) {}

// Enabled version
#[cfg(feature = "instrument")]
pub fn serialize_stats(output_path: impl AsRef<Path>) -> std::io::Result<()> {
    let output_file = File::create(output_path)?;

    let collected_stats = (*stats_collector().lock().unwrap()).clone();
    serde_json::to_writer_pretty(output_file, &collected_stats)?;
    Ok(())
}

// Disabled version
#[cfg(not(feature = "instrument"))]
pub fn serialize_stats(_output_path: impl AsRef<Path>) -> std::io::Result<()> {
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    pub label: String,
    pub times_before: TimeStats,
    pub times_after: TimeStats,
    pub mem_after: MemStats,
}

impl Stats {
    pub fn durations(&self) -> TimeStats {
        self.times_after.clone() - self.times_before.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeStats {
    pub uptime_secs: f64,
    pub user_cpu_secs: f64,
    pub system_cpu_secs: f64,
}

impl Sub for TimeStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            uptime_secs: self.uptime_secs - rhs.uptime_secs,
            user_cpu_secs: self.user_cpu_secs - rhs.user_cpu_secs,
            system_cpu_secs: self.system_cpu_secs - rhs.system_cpu_secs,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemStats {
    pub current_rss_bytes: u64,
    pub peak_rss_bytes: u64,
}

pub mod internal {
    use crate::instrumentation::{MemStats, Stats, TimeStats};
    use procfs::Current;
    use procfs::process::Process;
    use std::sync::{Mutex, OnceLock};

    #[doc(hidden)]
    pub static _COLLECTOR: OnceLock<Mutex<Vec<Stats>>> = OnceLock::new();

    #[cfg(feature = "instrument")]
    #[doc(hidden)]
    pub fn _get_mem_stats() -> MemStats {
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

        MemStats {
            current_rss_bytes,
            peak_rss_bytes,
        }
    }

    #[cfg(feature = "instrument")]
    #[doc(hidden)]
    pub fn _get_time_stats() -> TimeStats {
        let me = Process::myself().unwrap();
        let stat = me.stat().unwrap();

        let ticks = procfs::ticks_per_second() as f64;

        let start_secs = stat.starttime as f64 / ticks;
        let uptime = procfs::Uptime::current().unwrap().uptime;
        let uptime_secs = uptime - start_secs;

        let user_cpu_secs = stat.utime as f64 / ticks;
        let system_cpu_secs = stat.stime as f64 / ticks;

        TimeStats {
            uptime_secs,
            user_cpu_secs,
            system_cpu_secs,
        }
    }

    #[doc(hidden)]
    pub fn _assert_string<T: AsRef<str>>(_: &T) {}
}

// Enabled version
#[cfg(feature = "instrument")]
#[macro_export]
macro_rules! instrument {
    ($label:expr, $e:expr) => {{
        $crate::instrumentation::internal::_assert_string(&$label);

        let times_before = $crate::instrumentation::internal::_get_time_stats();

        let result = $e;

        let times_after = $crate::instrumentation::internal::_get_time_stats();
        let mem_after = $crate::instrumentation::internal::_get_mem_stats();

        $crate::instrumentation::stats_collector()
            .lock()
            .unwrap()
            .push($crate::instrumentation::Stats {
                label: $label.into(),
                times_before,
                times_after,
                mem_after,
            });

        result
    }};
}

// Disabled version
#[cfg(not(feature = "instrument"))]
#[macro_export]
macro_rules! instrument {
    ($label:expr, $e:expr) => {{
        $crate::instrumentation::internal::_assert_string(&$label);
        $e
    }};
}
