use rusqlite::{params, Connection, Result};

fn main() -> Result<()> {
    // Connect to or create a new database
    let conn = Connection::open("db.sqlite3")?;
}
