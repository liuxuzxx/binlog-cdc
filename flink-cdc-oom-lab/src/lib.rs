#![forbid(unsafe_code)]

mod memory;
mod metrics;
mod model;
mod observability;
mod producer;
mod scenario;

pub use memory::{
    CgroupMemoryEvents, MemoryObservation, ProcessMemorySnapshot, SmapsSnapshot,
    parse_cgroup_memory_events, parse_proc_status, parse_smaps_rollup, read_memory_observation,
};
pub use metrics::{KafkaQueueSnapshot, LabMetrics, MetricsSnapshot, SendOutcome};
pub use model::{EventGenerator, Operation, PayloadProfile, SimulatedEvent};
pub use observability::{JemallocSnapshot, read_jemalloc_snapshot, render_metrics};
pub use producer::{KafkaSink, ProducerSettings, QueuePolicy, classify_enqueue_error};
pub use scenario::{Phase, RateBudget, Scenario, ScenarioError};
