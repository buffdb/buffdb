use rusqlite::{Connection, Result as SqliteResult, Error as SqliteError};

fn main() -> SqliteResult<()> {
    // Connect to or create a new database
    let _conn = Connection::open("db.sqlite3")?;

    Ok(())
}
