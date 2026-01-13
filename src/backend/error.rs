use thiserror::Error;

#[derive(Error, Debug)]
pub enum BackendError {
    #[error("unable to have both locations for blob and kv as in memory")]
    BothLocationInMemory,
    #[error("generic backend error: `{0}`")]
    Generic(String),
    #[error("unknown backend error")]
    Unknown,
}
