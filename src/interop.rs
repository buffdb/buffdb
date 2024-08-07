//! Interoperability helpers for third-party to third-party conversions.

use crate::conv::Unsupported;
#[cfg(any(feature = "duckdb", feature = "sqlite"))]
use std::sync::Arc;
use tonic::Status;

/// Helper function to avoid writing `IntoTonicStatus::into_tonic_status` everywhere.
pub(crate) fn into_tonic_status<E: IntoTonicStatus>(err: E) -> Status {
    err.into_tonic_status()
}

/// Convert an error into `tonic::Status`.
pub trait IntoTonicStatus {
    /// Convert an error into `tonic::Status`.
    fn into_tonic_status(self) -> Status;
}

#[cfg(feature = "duckdb")]
impl IntoTonicStatus for duckdb::Error {
    fn into_tonic_status(self) -> Status {
        let mut tonic_err = match &self {
            Self::DuckDBFailure(a, b) => Status::internal(format!("DuckDB failure: {a} {b:?}")),
            Self::FromSqlConversionFailure(_, ty, _) => {
                Status::internal(format!("could not convert {ty} to Rust type"))
            }
            Self::IntegralValueOutOfRange(_, val) => {
                Status::out_of_range(format!("integral value {val} out of range"))
            }
            Self::Utf8Error(err) => Status::data_loss(format!("UTF-8 error: {err}")),
            Self::NulError(err) => Status::data_loss(format!("NUL error: {err}")),
            Self::InvalidParameterName(name) => {
                Status::internal(format!("invalid parameter name {name}"))
            }
            Self::InvalidPath(path) => Status::internal(format!("invalid path {}", path.display())),
            Self::ExecuteReturnedResults => Status::internal("`execute` returned results"),
            Self::QueryReturnedNoRows => Status::internal("`query` returned no rows"),
            Self::InvalidColumnIndex(idx) => {
                Status::internal(format!("invalid column index {idx}"))
            }
            Self::InvalidColumnName(name) => {
                Status::internal(format!("invalid column name {name}"))
            }
            Self::InvalidColumnType(_, name, ty) => {
                Status::internal(format!("invalid column type {ty} for column {name}"))
            }
            Self::ArrowTypeToDuckdbType(_, ty) => {
                Status::internal(format!("could not convert Arrow type {ty} to DuckDB type"))
            }
            Self::StatementChangedRows(count) => Status::internal(format!(
                "statement expected to change one row, but changed {count} rows"
            )),
            Self::ToSqlConversionFailure(err) => {
                Status::internal(format!("could not convert to SQL: {err}"))
            }
            Self::InvalidQuery => Status::internal("invalid query"),
            Self::MultipleStatement => Status::internal("multiple statements"),
            Self::InvalidParameterCount(given, expected) => Status::internal(format!(
                "invalid parameter count: expected {expected} but got {given}"
            )),
            Self::AppendError => Status::internal("append error"),
            _ => Status::unknown("unknown error"),
        };
        let _status = tonic_err.set_source(Arc::new(self));
        tonic_err
    }
}

#[cfg(feature = "sqlite")]
impl IntoTonicStatus for rusqlite::Error {
    fn into_tonic_status(self) -> Status {
        use rusqlite::ffi::ErrorCode;
        let mut tonic_err = match &self {
            Self::SqliteFailure(err, _) => match err.code {
                ErrorCode::NotFound => Status::not_found("not found"),
                ErrorCode::InternalMalfunction => Status::internal("internal malfunction"),
                ErrorCode::PermissionDenied => Status::permission_denied("permission denied"),
                ErrorCode::OperationAborted => Status::aborted("operation aborted"),
                ErrorCode::DatabaseBusy => Status::unavailable("database busy"),
                ErrorCode::DatabaseLocked => Status::internal("database locked"),
                ErrorCode::OutOfMemory => Status::resource_exhausted("out of memory"),
                ErrorCode::ReadOnly => Status::invalid_argument("read only"),
                ErrorCode::OperationInterrupted => Status::unavailable("operation interrupted"),
                ErrorCode::SystemIoFailure => Status::internal("system io failure"),
                ErrorCode::DatabaseCorrupt => Status::data_loss("database corrupt"),
                ErrorCode::DiskFull => Status::resource_exhausted("disk full"),
                ErrorCode::CannotOpen => Status::not_found("cannot open"),
                ErrorCode::FileLockingProtocolFailed => {
                    Status::internal("file locking protocol failed")
                }
                ErrorCode::SchemaChanged => Status::internal("schema changed"),
                ErrorCode::TooBig => Status::resource_exhausted("too big"),
                ErrorCode::ConstraintViolation => Status::invalid_argument("constraint violation"),
                ErrorCode::TypeMismatch => Status::invalid_argument("type mismatch"),
                ErrorCode::ApiMisuse => Status::internal("api misuse"),
                ErrorCode::NoLargeFileSupport => Status::invalid_argument("no large file support"),
                ErrorCode::AuthorizationForStatementDenied => {
                    Status::unauthenticated("authorization for statement denied")
                }
                ErrorCode::ParameterOutOfRange => Status::out_of_range("parameter out of range"),
                ErrorCode::NotADatabase => Status::invalid_argument("not a database"),
                ErrorCode::Unknown => Status::unknown("unknown"),
                _ => Status::unknown("unknown"),
            },
            Self::SqliteSingleThreadedMode => Status::internal("sqlite single threaded mode"),
            Self::FromSqlConversionFailure(_, _, err) => {
                Status::invalid_argument(format!("could not convert from SQL: {err}"))
            }
            Self::IntegralValueOutOfRange(_, val) => {
                Status::out_of_range(format!("integral value {val} out of range"))
            }
            Self::Utf8Error(err) => Status::failed_precondition(format!("UTF-8 error: {err}")),
            Self::NulError(err) => Status::failed_precondition(format!("NUL error: {err}")),
            Self::InvalidParameterName(name) => {
                Status::invalid_argument(format!("invalid parameter name {name}"))
            }
            Self::InvalidPath(path) => {
                Status::invalid_argument(format!("invalid path {}", path.display()))
            }
            Self::ExecuteReturnedResults => Status::invalid_argument("`execute` returned results"),
            Self::QueryReturnedNoRows => Status::invalid_argument("`query` returned no rows"),
            Self::InvalidColumnIndex(idx) => {
                Status::invalid_argument(format!("invalid column index {idx}"))
            }
            Self::InvalidColumnName(name) => {
                Status::invalid_argument(format!("invalid column name {name}"))
            }
            Self::InvalidColumnType(_, name, ty) => Status::invalid_argument(format!(
                "column {name} has type {ty}, which cannot be converted to a Rust type"
            )),
            Self::StatementChangedRows(num) => Status::invalid_argument(format!(
                "statement expected to change one row, but changed {num} rows"
            )),
            Self::ToSqlConversionFailure(err) => {
                Status::invalid_argument(format!("could not convert Rust type to SQL: {err}"))
            }
            Self::InvalidQuery => Status::invalid_argument("invalid query"),
            Self::UnwindingPanic => Status::internal("unwinding panic in user-defined function"),
            Self::MultipleStatement => Status::invalid_argument("multiple statements"),
            Self::InvalidParameterCount(given, expected) => Status::invalid_argument(format!(
                "invalid parameter count: expected {expected} but got {given}"
            )),
            _ => Status::unknown("unknown error"),
        };
        let _status = tonic_err.set_source(Arc::new(self));
        tonic_err
    }
}

impl IntoTonicStatus for prost::UnknownEnumValue {
    fn into_tonic_status(self) -> Status {
        Status::invalid_argument(format!("unknown enumeration value {}", self.0))
    }
}

impl IntoTonicStatus for Unsupported {
    fn into_tonic_status(self) -> Status {
        Status::unimplemented(format!("unsupported type: {}", self.message))
    }
}
