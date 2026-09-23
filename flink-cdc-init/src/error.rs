use thiserror::Error;

pub type Result<T> = std::result::Result<T, InitError>;

#[derive(Debug, Error)]
pub enum InitError {
    #[error("config error: {0}")]
    Config(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Yaml(#[from] serde_yaml::Error),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("kafka error: {0}")]
    Kafka(String),

    #[error("task join error: {0}")]
    Join(String),
}
