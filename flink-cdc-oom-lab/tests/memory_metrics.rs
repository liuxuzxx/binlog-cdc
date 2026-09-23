use flink_cdc_oom_lab::{parse_cgroup_memory_events, parse_proc_status, parse_smaps_rollup};

#[test]
fn parses_process_memory_in_bytes() {
    let status = r#"
Name:flink-cdc-rs
VmRSS:  508140 kB
RssAnon: 496500 kB
RssFile:  11640 kB
VmData: 573440 kB
"#;

    let memory = parse_proc_status(status);

    assert_eq!(memory.vm_rss_bytes, 508_140 * 1024);
    assert_eq!(memory.rss_anon_bytes, 496_500 * 1024);
    assert_eq!(memory.rss_file_bytes, 11_640 * 1024);
    assert_eq!(memory.vm_data_bytes, 573_440 * 1024);
}

#[test]
fn parses_anon_huge_pages_from_smaps_rollup() {
    let smaps = r#"
Rss:              520000 kB
Anonymous:        509000 kB
AnonHugePages:    390000 kB
"#;

    let memory = parse_smaps_rollup(smaps);

    assert_eq!(memory.anonymous_bytes, 509_000 * 1024);
    assert_eq!(memory.anon_huge_pages_bytes, 390_000 * 1024);
}

#[test]
fn parses_cgroup_oom_counters() {
    let events = r#"
low 0
high 0
max 111
oom 3
oom_kill 2
oom_group_kill 0
"#;

    let counters = parse_cgroup_memory_events(events);

    assert_eq!(counters.max, 111);
    assert_eq!(counters.oom, 3);
    assert_eq!(counters.oom_kill, 2);
}
