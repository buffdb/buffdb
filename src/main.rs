use rusqlite::{Connection, Error as SqliteError, Result as SqliteResult};

mod store {
    mod blob;
    mod kv;
    mod stream;
}

fn main() -> SqliteResult<()> {
    // Connect to or create a new database
    let _conn = Connection::open("db.sqlite3")?;

    Ok(())
}
