use crate::interop::IntoTonicStatus;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("Sqlite operation failed: {source}")]
pub struct SqliteError {
    #[from]
    pub source: rusqlite::Error,
}

impl From<rusqlite::Error> for BackendError {
    fn from(value: rusqlite::Error) -> Self {
        Self::RusQliteError(SqliteError { source: value })
    }
}

impl BackendError {
    pub fn sqlite(err: rusqlite::Error) -> Self {
        Self::RusQliteError(SqliteError { source: err })
    }
}

impl IntoTonicStatus for BackendError {
    fn into_tonic_status(self) -> tonic::Status {
        match self {
            Self::Connection { message } => tonic::Status::failed_precondition(format!(
                "Unable to establish connection: {message}"
            )),
            Self::InvalidLocation { message } => tonic::Status::invalid_argument(message),
            Self::RusQliteError(db_err) => match db_err.source {
                rusqlite::Error::QueryReturnedNoRows => {
                    tonic::Status::not_found("record not found in database")
                }
                rusqlite::Error::SqliteFailure(e, msg) => match e.code {
                    rusqlite::ErrorCode::ConstraintViolation => tonic::Status::already_exists(
                        msg.unwrap_or_else(|| "Constraint violation".into()),
                    ),
                    _ => tonic::Status::internal(format!("Database error: {e} {msg:?}")),
                },
                _ => {
                    tonic::Status::internal(format!("Unhandled database error: {}", db_err.source))
                }
            },
            Self::Initialization { message } => tonic::Status::internal(message),
            Self::Generic { message } => tonic::Status::internal(message),
            Self::Unknown => tonic::Status::internal("An unknown error occured"),
        }
    }
}

#[derive(Error, Debug)]
pub enum BackendError {
    #[error("connection error: `{message}`")]
    Connection { message: String },
    #[error("rusqlite error: `{0}`")]
    RusQliteError(#[from] SqliteError),
    #[error("invalid location: `{message}`")]
    InvalidLocation { message: String },
    #[error("initialization error: `{message}`")]
    Initialization { message: String },
    #[error("generic backend error: `{message}`")]
    Generic { message: String },
    #[error("unknown backend error")]
    Unknown,
}
