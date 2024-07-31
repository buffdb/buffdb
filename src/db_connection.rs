use duckdb::Connection;
use std::path::PathBuf;

pub(crate) type Database = Connection;

/// The location of a database.
#[non_exhaustive] // future-proofing for options like network storage
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Location {
    /// An in-memory database. This is useful for short-lived data.
    InMemory,
    /// A database stored on the disk. This is useful for long-lived data.
    OnDisk {
        /// The path to the database. This is permitted to be a path to a network file system, if
        /// desired.
        path: PathBuf,
    },
}

impl<T> From<T> for Location
where
    T: Into<PathBuf>,
{
    fn from(path: T) -> Self {
        Self::OnDisk { path: path.into() }
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
