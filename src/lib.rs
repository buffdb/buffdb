pub mod blob;
mod db_connection;
mod interop;
pub mod kv;

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

pub type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;

pub use crate::db_connection::Location;
