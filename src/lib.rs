pub mod blob;
mod db_connection;
mod helpers;
mod interop;
pub mod kv;
pub mod transitive;

pub mod schema {
    pub(crate) mod blob {
        tonic::include_proto!("blob");
    }
    pub mod common {
        tonic::include_proto!("common");
    }
    pub(crate) mod kv {
        tonic::include_proto!("kv");
    }
}

pub use crate::db_connection::Location;
use futures::Stream;
use std::pin::Pin;

pub type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;
type StreamingRequest<T> = tonic::Request<tonic::Streaming<T>>;
type DynStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;
