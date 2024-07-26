pub mod blob;
mod db_connection;
pub mod kv;

pub type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;

pub use crate::db_connection::Location;
