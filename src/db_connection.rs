use duckdb::Connection;
use std::path::PathBuf;

pub(crate) type Database = Connection;

/// The location of a database.
#[non_exhaustive] // future-proofing for options like network storage
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Location {
    InMemory,
    OnDisk { path: PathBuf },
}

pub(crate) trait DbConnectionInfo {
    fn initialize(db: &Database) -> duckdb::Result<()>;

    fn location(&self) -> &Location;

    /// Open a connection to the database.
    fn connect(&self) -> Database {
        let conn = match &self.location() {
            Location::InMemory => Connection::open_in_memory().unwrap(),
            Location::OnDisk { path } => Connection::open(path).unwrap(),
        };

        // TODO Wrap this in a boolean to indicate whether this has already been run. This avoids
        // running this every time a connection is opened.
        Self::initialize(&conn).unwrap();
        conn
    }
}
