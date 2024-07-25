#![allow(unused)] // TODO temporary

pub mod store;

use rusqlite::{Connection, Error as SqliteError, Result as SqliteResult};

type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;

fn main() -> SqliteResult<()> {
    // Connect to or create a new database
    let _conn = Connection::open("db.sqlite3")?;

    Ok(())
}
