use duckdb::Error;
use std::sync::Arc;
use tonic::Status;

pub(crate) fn duckdb_err_to_tonic_status(duckdb_err: Error) -> Status {
    let mut tonic_err = match &duckdb_err {
        Error::DuckDBFailure(a, b) => Status::internal(format!("DuckDB failure: {a} {b:?}")),
        Error::FromSqlConversionFailure(_, ty, _) => {
            Status::internal(format!("could not convert {ty} to Rust type"))
        }
        Error::IntegralValueOutOfRange(_, val) => {
            Status::out_of_range(format!("integral value {val} out of range"))
        }
        Error::Utf8Error(err) => Status::data_loss(format!("UTF-8 error: {err}")),
        Error::NulError(err) => Status::data_loss(format!("NUL error: {err}")),
        Error::InvalidParameterName(name) => {
            Status::internal(format!("invalid parameter name {name}"))
        }
        Error::InvalidPath(path) => Status::internal(format!("invalid path {}", path.display())),
        Error::ExecuteReturnedResults => Status::internal("`execute` returned results"),
        Error::QueryReturnedNoRows => Status::internal("`query` returned no rows"),
        Error::InvalidColumnIndex(idx) => Status::internal(format!("invalid column index {idx}")),
        Error::InvalidColumnName(name) => Status::internal(format!("invalid column name {name}")),
        Error::InvalidColumnType(_, name, ty) => {
            Status::internal(format!("invalid column type {ty} for column {name}"))
        }
        Error::ArrowTypeToDuckdbType(_, ty) => {
            Status::internal(format!("could not convert Arrow type {ty} to DuckDB type"))
        }
        Error::StatementChangedRows(count) => Status::internal(format!(
            "statement expected to change one row, but changed {count} rows"
        )),
        Error::ToSqlConversionFailure(err) => {
            Status::internal(format!("could not convert to SQL: {err}"))
        }
        Error::InvalidQuery => Status::internal("invalid query"),
        Error::MultipleStatement => Status::internal("multiple statements"),
        Error::InvalidParameterCount(given, expected) => Status::internal(format!(
            "invalid parameter count: expected {expected} but got {given}"
        )),
        Error::AppendError => Status::internal("append error"),
        _ => Status::unknown("unknown error"),
    };
    let _status = tonic_err.set_source(Arc::new(duckdb_err));
    tonic_err
}
