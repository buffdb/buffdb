use std::path::PathBuf;

pub(crate) type Database = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

/// The location of a database.
///
/// To store data in memory, use `OnDisk` with a path pointing to a tmpfs or ramfs.
#[non_exhaustive] // future-proofing for options like network storage
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Location {
    OnDisk { path: PathBuf },
}

pub(crate) trait DbConnectionInfo {
    /// Adjust database options before opening it.
    fn adjust_opts(_opts: &mut rocksdb::Options) {}

    /// The column families to create and open.
    ///
    /// The default implementation returns `None`, which means that only the default (anonymous)
    /// column family is present.
    fn fields() -> Option<impl Iterator<Item = &'static str>> {
        None::<std::array::IntoIter<&str, 0>>
    }

    /// Open a connection to the database.
    fn connection(location: Location) -> Database {
        match location {
            Location::OnDisk { path } => {
                let mut opts = rocksdb::Options::default();
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
                Self::adjust_opts(&mut opts);

                match Self::fields() {
                    None => Database::open(&opts, path).unwrap(),
                    Some(fields) => Database::open_cf(&opts, path, fields).unwrap(),
                }
            }
        }
    }
}
