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

impl From<PathBuf> for Location {
    fn from(path: PathBuf) -> Self {
        Self::OnDisk { path }
    }
}

pub(crate) trait DbConnectionInfo {
    fn initialize(db: &Database) -> duckdb::Result<()>;

    fn location(&self) -> &Location;

    /// Open a connection to the database.
    fn connect(&self) -> duckdb::Result<Database> {
        let conn = match &self.location() {
            Location::InMemory => Connection::open_in_memory(),
            Location::OnDisk { path } => Connection::open(path),
        }?;

        // TODO Wrap this in a boolean to indicate whether this has already been run. This avoids
        // running this every time a connection is opened.
        Self::initialize(&conn)?;
        Ok(conn)
    }
}
