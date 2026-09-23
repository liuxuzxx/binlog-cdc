use std::{fs, path::Path};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessMemorySnapshot {
    pub vm_rss_bytes: u64,
    pub rss_anon_bytes: u64,
    pub rss_file_bytes: u64,
    pub vm_data_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SmapsSnapshot {
    pub anonymous_bytes: u64,
    pub anon_huge_pages_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CgroupMemoryEvents {
    pub max: u64,
    pub oom: u64,
    pub oom_kill: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryObservation {
    pub process: ProcessMemorySnapshot,
    pub smaps: SmapsSnapshot,
    pub cgroup_current_bytes: u64,
    pub cgroup_peak_bytes: u64,
    pub cgroup_events: CgroupMemoryEvents,
}

pub fn parse_proc_status(input: &str) -> ProcessMemorySnapshot {
    ProcessMemorySnapshot {
        vm_rss_bytes: kib_value(input, "VmRSS"),
        rss_anon_bytes: kib_value(input, "RssAnon"),
        rss_file_bytes: kib_value(input, "RssFile"),
        vm_data_bytes: kib_value(input, "VmData"),
    }
}

pub fn parse_smaps_rollup(input: &str) -> SmapsSnapshot {
    SmapsSnapshot {
        anonymous_bytes: kib_value(input, "Anonymous"),
        anon_huge_pages_bytes: kib_value(input, "AnonHugePages"),
    }
}

pub fn parse_cgroup_memory_events(input: &str) -> CgroupMemoryEvents {
    let mut events = CgroupMemoryEvents::default();
    for line in input.lines() {
        let mut fields = line.split_whitespace();
        let Some(name) = fields.next() else {
            continue;
        };
        let value = fields
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default();
        match name {
            "max" => events.max = value,
            "oom" => events.oom = value,
            "oom_kill" => events.oom_kill = value,
            _ => {}
        }
    }
    events
}

pub fn read_memory_observation() -> MemoryObservation {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let smaps = fs::read_to_string("/proc/self/smaps_rollup").unwrap_or_default();

    MemoryObservation {
        process: parse_proc_status(&status),
        smaps: parse_smaps_rollup(&smaps),
        cgroup_current_bytes: read_u64("/sys/fs/cgroup/memory.current"),
        cgroup_peak_bytes: read_u64("/sys/fs/cgroup/memory.peak"),
        cgroup_events: fs::read_to_string("/sys/fs/cgroup/memory.events")
            .map(|value| parse_cgroup_memory_events(&value))
            .unwrap_or_default(),
    }
}

fn kib_value(input: &str, target: &str) -> u64 {
    input
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| *name == target)
        .and_then(|(_, value)| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default()
        .saturating_mul(1024)
}

fn read_u64(path: impl AsRef<Path>) -> u64 {
    fs::read_to_string(path)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or_default()
}
