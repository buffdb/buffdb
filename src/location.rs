use std::path::PathBuf;

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
