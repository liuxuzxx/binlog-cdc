use rand::{
    Rng, SeedableRng,
    distributions::{Alphanumeric, DistString},
    rngs::StdRng,
};
use serde::{
    Serialize,
    ser::{SerializeStruct, Serializer},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

impl Operation {
    fn code(self) -> &'static str {
        match self {
            Self::Insert => "c",
            Self::Update => "u",
            Self::Delete => "d",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadProfile {
    Fixed(usize),
    Mixed {
        small: usize,
        medium: usize,
        large: usize,
    },
}

impl PayloadProfile {
    fn next_size(self, rng: &mut StdRng) -> usize {
        match self {
            Self::Fixed(size) => size,
            Self::Mixed {
                small,
                medium,
                large,
            } => match rng.gen_range(0..100) {
                0..=79 => small,
                80..=94 => medium,
                _ => large,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulatedEvent {
    pub sequence: u64,
    pub operation: Operation,
    pub content: String,
}

#[derive(Serialize)]
struct Source<'a> {
    db: &'a str,
    table: &'a str,
    file: &'a str,
    pos: u64,
}

#[derive(Serialize)]
struct Row<'a> {
    id: u64,
    content: &'a str,
}

impl Serialize for SimulatedEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let row = Row {
            id: self.sequence,
            content: &self.content,
        };
        let (before, after) = match self.operation {
            Operation::Insert => (None, Some(&row)),
            Operation::Update => (Some(&row), Some(&row)),
            Operation::Delete => (Some(&row), None),
        };
        let source = Source {
            db: "oom_lab",
            table: "simulated_binlog",
            file: "mysql-bin.000001",
            pos: self.sequence,
        };

        let mut state = serializer.serialize_struct("DebeziumEnvelope", 5)?;
        state.serialize_field("before", &before)?;
        state.serialize_field("after", &after)?;
        state.serialize_field("source", &source)?;
        state.serialize_field("op", self.operation.code())?;
        state.serialize_field("ts_ms", &(1_700_000_000_000_u64 + self.sequence))?;
        state.end()
    }
}

pub struct EventGenerator {
    rng: StdRng,
    profile: PayloadProfile,
    sequence: u64,
}

impl EventGenerator {
    pub fn new(seed: u64, profile: PayloadProfile) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            profile,
            sequence: 0,
        }
    }

    pub fn next_event(&mut self) -> SimulatedEvent {
        self.sequence += 1;
        let operation = match self.rng.gen_range(0..3) {
            0 => Operation::Insert,
            1 => Operation::Update,
            _ => Operation::Delete,
        };
        let size = self.profile.next_size(&mut self.rng);
        let content = Alphanumeric.sample_string(&mut self.rng, size);

        SimulatedEvent {
            sequence: self.sequence,
            operation,
            content,
        }
    }
}
