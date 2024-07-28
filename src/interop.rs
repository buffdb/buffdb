pub(crate) fn rocksdb_err_to_tonic_status(rocksdb_err: rocksdb::Error) -> tonic::Status {
    use rocksdb::ErrorKind;
    use std::sync::Arc;
    use tonic::Status;

    let mut tonic_err = match rocksdb_err.kind() {
        ErrorKind::NotFound => Status::not_found("not found"),
        ErrorKind::Corruption => Status::data_loss("data is corrupted"),
        ErrorKind::NotSupported => Status::unimplemented("operation not supported"),
        ErrorKind::InvalidArgument => Status::invalid_argument("invalid argument"),
        ErrorKind::IOError => Status::internal("I/O error"),
        ErrorKind::MergeInProgress => Status::aborted("merge in progress"),
        ErrorKind::Incomplete => Status::internal("incomplete operation"),
        ErrorKind::ShutdownInProgress => Status::unavailable("shutdown in progress"),
        ErrorKind::TimedOut => Status::deadline_exceeded("operation timed out"),
        ErrorKind::Aborted => Status::aborted("operation aborted"),
        ErrorKind::Busy => Status::unavailable("server is busy"),
        ErrorKind::Expired => Status::deadline_exceeded("operation expired"),
        ErrorKind::TryAgain => Status::unavailable("try again"),
        ErrorKind::CompactionTooLarge => Status::internal("compaction too large"),
        ErrorKind::ColumnFamilyDropped => Status::internal("column family dropped"),
        ErrorKind::Unknown => Status::unknown("unknown error"),
    };
    tonic_err.set_source(Arc::new(rocksdb_err));
    tonic_err
}
