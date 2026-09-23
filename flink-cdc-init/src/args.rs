use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, short)]
    flink_cdc: String,
}

impl Args {
    pub fn flink_cdc(&self) -> &str {
        self.flink_cdc.as_str()
    }
}

impl std::fmt::Display for Args {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "flink_cdc: {}", self.flink_cdc)
    }
}
