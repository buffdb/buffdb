pub trait FrontendError {
    fn internal(message: String) -> Self;
    fn out_of_range(message: String) -> Self;
    fn data_loss(message: String) -> Self;
    fn not_found(message: String) -> Self;
    fn permission_denied(message: String) -> Self;
    fn aborted(message: String) -> Self;
    fn unavailable(message: String) -> Self;
    fn resource_exhausted(message: String) -> Self;
    fn invalid_argument(message: String) -> Self;
    fn unknown(message: String) -> Self;
    fn failed_precondition(message: String) -> Self;
    fn unimplemented(message: String) -> Self;
    fn timed_out(message: String) -> Self;
}

impl FrontendError for tonic::Status {
    fn internal(message: String) -> Self {
        tonic::Status::internal(message)
    }

    fn out_of_range(message: String) -> Self {
        tonic::Status::out_of_range(message)
    }

    fn data_loss(message: String) -> Self {
        tonic::Status::data_loss(message)
    }

    fn not_found(message: String) -> Self {
        tonic::Status::not_found(message)
    }

    fn permission_denied(message: String) -> Self {
        tonic::Status::permission_denied(message)
    }

    fn aborted(message: String) -> Self {
        tonic::Status::aborted(message)
    }

    fn unavailable(message: String) -> Self {
        tonic::Status::unavailable(message)
    }

    fn resource_exhausted(message: String) -> Self {
        tonic::Status::resource_exhausted(message)
    }

    fn invalid_argument(message: String) -> Self {
        tonic::Status::invalid_argument(message)
    }

    fn unknown(message: String) -> Self {
        tonic::Status::unknown(message)
    }

    fn failed_precondition(message: String) -> Self {
        tonic::Status::failed_precondition(message)
    }

    fn unimplemented(message: String) -> Self {
        tonic::Status::unimplemented(message)
    }

    fn timed_out(message: String) -> Self {
        tonic::Status::deadline_exceeded(message)
    }
}
