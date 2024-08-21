//! Interoperability helpers for third-party to third-party conversions.

use crate::conv::grpc::Unsupported;
#[cfg(any(feature = "duckdb", feature = "sqlite"))]
use std::sync::Arc;
use tonic::Status;

#[repr(transparent)]
#[derive(Debug)]
pub struct DatabaseError<T>(pub T);

#[cfg(feature = "duckdb")]
impl From<duckdb::Error> for DatabaseError<Status> {
    fn from(error: duckdb::Error) -> DatabaseError<Status> {
        use duckdb::Error::*;
        let mut tonic_err = match &error {
            DuckDBFailure(a, b) => Status::internal(format!("DuckDB failure: {a} {b:?}")),
            FromSqlConversionFailure(_, ty, _) => {
                Status::internal(format!("could not convert {ty} to Rust type"))
            }
            IntegralValueOutOfRange(_, val) => {
                Status::out_of_range(format!("integral value {val} out of range"))
            }
            Utf8Error(err) => Status::data_loss(format!("UTF-8 error: {err}")),
            NulError(err) => Status::data_loss(format!("NUL error: {err}")),
            InvalidParameterName(name) => {
                Status::internal(format!("invalid parameter name {name}"))
            }
            InvalidPath(path) => Status::internal(format!("invalid path {}", path.display())),
            ExecuteReturnedResults => Status::internal("`execute` returned results"),
            QueryReturnedNoRows => Status::internal("`query` returned no rows"),
            InvalidColumnIndex(idx) => Status::internal(format!("invalid column index {idx}")),
            InvalidColumnName(name) => Status::internal(format!("invalid column name {name}")),
            InvalidColumnType(_, name, ty) => {
                Status::internal(format!("invalid column type {ty} for column {name}"))
            }
            ArrowTypeToDuckdbType(_, ty) => {
                Status::internal(format!("could not convert Arrow type {ty} to DuckDB type"))
            }
            StatementChangedRows(count) => Status::internal(format!(
                "statement expected to change one row, but changed {count} rows"
            )),
            ToSqlConversionFailure(err) => {
                Status::internal(format!("could not convert to SQL: {err}"))
            }
            InvalidQuery => Status::internal("invalid query"),
            MultipleStatement => Status::internal("multiple statements"),
            InvalidParameterCount(given, expected) => Status::internal(format!(
                "invalid parameter count: expected {expected} but got {given}"
            )),
            AppendError => Status::internal("append error"),
            _ => Status::unknown("unknown error"),
        };
        let _status = tonic_err.set_source(Arc::new(error));
        DatabaseError(tonic_err)
    }
}

#[cfg(feature = "sqlite")]
impl From<rusqlite::Error> for DatabaseError<Status> {
    fn from(error: rusqlite::Error) -> DatabaseError<Status> {
        use rusqlite::ffi::ErrorCode;
        let mut tonic_err = match &error {
            rusqlite::Error::SqliteFailure(err, msg) => match err.code {
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
                ErrorCode::Unknown => {
                    let message = match msg.as_ref() {
                        Some(message) => message,
                        None => "no message",
                    };
                    Status::unknown(format!("unknown: {message}"))
                }
                _ => Status::unknown("unknown"),
            },
            rusqlite::Error::SqliteSingleThreadedMode => {
                Status::internal("sqlite single threaded mode")
            }
            rusqlite::Error::FromSqlConversionFailure(_, _, err) => {
                Status::invalid_argument(format!("could not convert from SQL: {err}"))
            }
            rusqlite::Error::IntegralValueOutOfRange(_, val) => {
                Status::out_of_range(format!("integral value {val} out of range"))
            }
            rusqlite::Error::Utf8Error(err) => {
                Status::failed_precondition(format!("UTF-8 error: {err}"))
            }
            rusqlite::Error::NulError(err) => {
                Status::failed_precondition(format!("NUL error: {err}"))
            }
            rusqlite::Error::InvalidParameterName(name) => {
                Status::invalid_argument(format!("invalid parameter name {name}"))
            }
            rusqlite::Error::InvalidPath(path) => {
                Status::invalid_argument(format!("invalid path {}", path.display()))
            }
            rusqlite::Error::ExecuteReturnedResults => {
                Status::invalid_argument("`execute` returned results")
            }
            rusqlite::Error::QueryReturnedNoRows => {
                Status::invalid_argument("`query` returned no rows")
            }
            rusqlite::Error::InvalidColumnIndex(idx) => {
                Status::invalid_argument(format!("invalid column index {idx}"))
            }
            rusqlite::Error::InvalidColumnName(name) => {
                Status::invalid_argument(format!("invalid column name {name}"))
            }
            rusqlite::Error::InvalidColumnType(_, name, ty) => Status::invalid_argument(format!(
                "column {name} has type {ty}, which cannot be converted to a Rust type"
            )),
            rusqlite::Error::StatementChangedRows(num) => Status::invalid_argument(format!(
                "statement expected to change one row, but changed {num} rows"
            )),
            rusqlite::Error::ToSqlConversionFailure(err) => {
                Status::invalid_argument(format!("could not convert Rust type to SQL: {err}"))
            }
            rusqlite::Error::InvalidQuery => Status::invalid_argument("invalid query"),
            rusqlite::Error::UnwindingPanic => {
                Status::internal("unwinding panic in user-defined function")
            }
            rusqlite::Error::MultipleStatement => Status::invalid_argument("multiple statements"),
            rusqlite::Error::InvalidParameterCount(given, expected) => Status::invalid_argument(
                format!("invalid parameter count: expected {expected} but got {given}"),
            ),
            _ => Status::unknown("unknown error"),
        };
        let _status = tonic_err.set_source(Arc::new(error));
        DatabaseError(tonic_err)
    }
}

#[cfg(feature = "rocksdb")]
impl From<rocksdb::Error> for DatabaseError<Status> {
    fn from(error: rocksdb::Error) -> DatabaseError<Status> {
        let message = error.clone().into_string();
        DatabaseError(match error.kind() {
            rocksdb::ErrorKind::NotFound => Status::not_found(message),
            rocksdb::ErrorKind::Corruption => Status::data_loss(message),
            rocksdb::ErrorKind::NotSupported => Status::unimplemented(message),
            rocksdb::ErrorKind::InvalidArgument => Status::invalid_argument(message),
            rocksdb::ErrorKind::IOError => Status::internal(message),
            rocksdb::ErrorKind::MergeInProgress => Status::aborted(message),
            rocksdb::ErrorKind::Incomplete => Status::internal(message),
            rocksdb::ErrorKind::ShutdownInProgress => Status::unavailable(message),
            rocksdb::ErrorKind::TimedOut => Status::deadline_exceeded(message),
            rocksdb::ErrorKind::Aborted => Status::aborted(message),
            rocksdb::ErrorKind::Busy => Status::unavailable(message),
            rocksdb::ErrorKind::Expired => Status::deadline_exceeded(message),
            rocksdb::ErrorKind::TryAgain => Status::unavailable(message),
            rocksdb::ErrorKind::CompactionTooLarge => Status::resource_exhausted(message),
            rocksdb::ErrorKind::ColumnFamilyDropped => Status::internal(message),
            rocksdb::ErrorKind::Unknown => Status::unknown(message),
        })
    }
}

impl From<prost::UnknownEnumValue> for DatabaseError<Status> {
    fn from(error: prost::UnknownEnumValue) -> DatabaseError<Status> {
        DatabaseError(Status::invalid_argument(format!(
            "unknown enumeration value {}",
            error.0
        )))
    }
}

impl From<Unsupported> for DatabaseError<Status> {
    fn from(error: Unsupported) -> DatabaseError<Status> {
        DatabaseError(Status::unimplemented(format!(
            "unsupported type: {}",
            error.message
        )))
    }
}
