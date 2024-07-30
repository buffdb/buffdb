pub(crate) fn duckdb_err_to_tonic_status(duckdb_err: duckdb::Error) -> tonic::Status {
    use duckdb::Error;
    use std::sync::Arc;
    use tonic::Status;

    let mut tonic_err = match duckdb_err {
        Error::DuckDBFailure(_, _) => todo!(),
        Error::FromSqlConversionFailure(_, _, _) => todo!(),
        Error::IntegralValueOutOfRange(_, _) => todo!(),
        Error::Utf8Error(_) => todo!(),
        Error::NulError(_) => todo!(),
        Error::InvalidParameterName(_) => todo!(),
        Error::InvalidPath(_) => todo!(),
        Error::ExecuteReturnedResults => todo!(),
        Error::QueryReturnedNoRows => todo!(),
        Error::InvalidColumnIndex(_) => todo!(),
        Error::InvalidColumnName(_) => todo!(),
        Error::InvalidColumnType(_, _, _) => todo!(),
        Error::ArrowTypeToDuckdbType(_, _) => todo!(),
        Error::StatementChangedRows(_) => todo!(),
        Error::ToSqlConversionFailure(_) => todo!(),
        Error::InvalidQuery => todo!(),
        Error::MultipleStatement => todo!(),
        Error::InvalidParameterCount(_, _) => todo!(),
        Error::AppendError => todo!(),
        _ => Status::unknown("unknown error"),
        // Error::NotFound => Status::not_found("not found"),
        // Error::Corruption => Status::data_loss("data is corrupted"),
        // Error::NotSupported => Status::unimplemented("operation not supported"),
        // Error::InvalidArgument => Status::invalid_argument("invalid argument"),
        // Error::IOError => Status::internal("I/O error"),
        // Error::MergeInProgress => Status::aborted("merge in progress"),
        // Error::Incomplete => Status::internal("incomplete operation"),
        // Error::ShutdownInProgress => Status::unavailable("shutdown in progress"),
        // Error::TimedOut => Status::deadline_exceeded("operation timed out"),
        // Error::Aborted => Status::aborted("operation aborted"),
        // Error::Busy => Status::unavailable("server is busy"),
        // Error::Expired => Status::deadline_exceeded("operation expired"),
        // Error::TryAgain => Status::unavailable("try again"),
        // Error::CompactionTooLarge => Status::internal("compaction too large"),
        // Error::ColumnFamilyDropped => Status::internal("column family dropped"),
        // Error::Unknown => Status::unknown("unknown error"),
    };
    tonic_err.set_source(Arc::new(duckdb_err));
    tonic_err
}
