pub mod blob;
pub mod kv;
mod location;

pub type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;

pub use self::location::Location;
