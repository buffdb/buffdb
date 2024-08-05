use std::sync::Arc;
use tonic::Status;

/// Helper function to avoid writing `IntoTonicStatus::into_tonic_status` everywhere.
pub(crate) fn into_tonic_status<E: IntoTonicStatus>(err: E) -> Status {
    err.into_tonic_status()
}

pub trait IntoTonicStatus {
    fn into_tonic_status(self) -> Status;
}

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
