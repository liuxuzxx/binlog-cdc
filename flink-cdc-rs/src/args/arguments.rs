use clap::Parser;

#[derive(Parser, Debug)]
#[command(author,version,about,long_about = None)]
pub struct Args {
    #[arg(long, short)]
    flink_cdc: String,
}

impl Args {
    pub fn flink_cdc(&self) -> &str {
        return self.flink_cdc.as_str();
    }
}

impl ToString for Args {
    fn to_string(&self) -> String {
        return format!("flink_cdc: {}", self.flink_cdc);
    }
}
