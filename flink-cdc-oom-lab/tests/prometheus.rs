use flink_cdc_oom_lab::{
    CgroupMemoryEvents, JemallocSnapshot, LabMetrics, MemoryObservation, ProcessMemorySnapshot,
    SendOutcome, SmapsSnapshot, render_metrics,
};

#[test]
fn prometheus_output_contains_queue_allocator_and_cgroup_evidence() {
    let metrics = LabMetrics::default();
    metrics.record(SendOutcome::Attempted, 1024);
    metrics.record(SendOutcome::Serialized, 1200);
    let memory = MemoryObservation {
        process: ProcessMemorySnapshot {
            vm_rss_bytes: 500,
            rss_anon_bytes: 400,
            rss_file_bytes: 100,
            vm_data_bytes: 900,
        },
        smaps: SmapsSnapshot {
            anonymous_bytes: 410,
            anon_huge_pages_bytes: 300,
        },
        cgroup_current_bytes: 600,
        cgroup_peak_bytes: 700,
        cgroup_events: CgroupMemoryEvents {
            max: 3,
            oom: 2,
            oom_kill: 1,
        },
    };
    let jemalloc = JemallocSnapshot {
        allocated_bytes: 200,
        active_bytes: 250,
        resident_bytes: 450,
        retained_bytes: 50,
    };

    let output = render_metrics(&metrics.snapshot(), &memory, &jemalloc);

    assert!(output.contains("flink_cdc_oom_lab_attempted_total 1"));
    assert!(output.contains("flink_cdc_oom_lab_process_rss_anon_bytes 400"));
    assert!(output.contains("flink_cdc_oom_lab_anon_huge_pages_bytes 300"));
    assert!(output.contains("flink_cdc_oom_lab_cgroup_memory_peak_bytes 700"));
    assert!(output.contains("flink_cdc_oom_lab_cgroup_oom_kill_total 1"));
    assert!(output.contains("flink_cdc_oom_lab_jemalloc_resident_bytes 450"));
}
