//! Configuration management for BuffDB.
//!
//! This module provides configuration structures and loading logic for BuffDB.
//! It supports loading configuration from TOML files with CLI argument overrides.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Backend enum for configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigBackend {
    Sqlite,
    #[cfg(feature = "duckdb")]
    DuckDb,
}

impl std::fmt::Display for ConfigBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite => write!(f, "sqlite"),
            #[cfg(feature = "duckdb")]
            Self::DuckDb => write!(f, "duckdb"),
        }
    }
}

/// Configuration error types.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Failed to parse TOML: {0}")]
    TomlParseError(#[from] toml::de::Error),
    #[error("Invalid address format: {0}")]
    InvalidAddress(String),
    #[error("Invalid backend: {0}")]
    InvalidBackend(String),
    #[error("Configuration validation failed: {0}")]
    ValidationError(String),
}

/// Server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// The address to bind the gRPC server to.
    pub address: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: "[::1]:9313".to_string(),
        }
    }
}

/// Database configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// The backend to use for BuffDB.
    pub backend: String,
    /// The location of the key-value store.
    pub kv_store: PathBuf,
    /// The location of the BLOB store.
    pub blob_store: PathBuf,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            backend: "sqlite".to_string(),
            kv_store: PathBuf::from("kv_store.db"),
            blob_store: PathBuf::from("blob_store.db"),
        }
    }
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// The logging level.
    pub level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}

/// Performance configuration.
#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Maximum number of concurrent connections.
    pub max_connections: Option<usize>,
    /// Request timeout in seconds.
    pub request_timeout: Option<u64>,
    /// Connection keep-alive interval in seconds.
    pub keep_alive: Option<u64>,
}

/// Main BuffDB configuration.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Server configuration.
    pub server: ServerConfig,
    /// Database configuration.
    pub database: DatabaseConfig,
    /// Logging configuration.
    pub logging: LoggingConfig,
    /// Performance configuration.
    pub performance: PerformanceConfig,
}

impl Config {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load configuration from a TOML file.
    pub fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    /// Load configuration from a TOML file if it exists, otherwise return defaults.
    pub fn from_file_or_default<P: AsRef<std::path::Path>>(path: P) -> Result<Self, ConfigError> {
        if path.as_ref().exists() {
            Self::from_file(path)
        } else {
            Ok(Self::default())
        }
    }

    /// Get the server address as a SocketAddr.
    pub fn server_address(&self) -> Result<SocketAddr, ConfigError> {
        SocketAddr::from_str(&self.server.address)
            .map_err(|_| ConfigError::InvalidAddress(self.server.address.clone()))
    }

    /// Get the backend as a ConfigBackend enum.
    pub fn backend(&self) -> Result<ConfigBackend, ConfigError> {
        match self.database.backend.to_lowercase().as_str() {
            "sqlite" => Ok(ConfigBackend::Sqlite),
            #[cfg(feature = "duckdb")]
            "duckdb" => Ok(ConfigBackend::DuckDb),
            backend => Err(ConfigError::InvalidBackend(backend.to_string())),
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Validate server address
        _ = self.server_address()?;

        // Validate backend
        _ = self.backend()?;

        // Validate that kv_store and blob_store are different
        if self.database.kv_store == self.database.blob_store {
            return Err(ConfigError::ValidationError(
                "kv_store and blob_store cannot be the same path".to_string(),
            ));
        }

        // Check if paths are the same file on Unix systems
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if let (Ok(kv_meta), Ok(blob_meta)) = (
                std::fs::metadata(&self.database.kv_store),
                std::fs::metadata(&self.database.blob_store),
            ) {
                if kv_meta.ino() == blob_meta.ino() {
                    return Err(ConfigError::ValidationError(
                        "kv_store and blob_store cannot be the same file".to_string(),
                    ));
                }
            }
        }

        // Validate logging level
        match self.logging.level.to_lowercase().as_str() {
            "trace" | "debug" | "info" | "warn" | "error" => {}
            level => {
                return Err(ConfigError::ValidationError(format!(
                    "Invalid logging level: {level}. Valid levels: trace, debug, info, warn, error"
                )));
            }
        }

        Ok(())
    }

    /// Apply CLI argument overrides to this configuration.
    pub fn apply_cli_overrides(
        &mut self,
        backend: Option<ConfigBackend>,
        kv_store: Option<PathBuf>,
        blob_store: Option<PathBuf>,
        address: Option<SocketAddr>,
    ) {
        if let Some(backend) = backend {
            self.database.backend = match backend {
                ConfigBackend::Sqlite => "sqlite".to_string(),
                #[cfg(feature = "duckdb")]
                ConfigBackend::DuckDb => "duckdb".to_string(),
            };
        }

        if let Some(kv_store) = kv_store {
            self.database.kv_store = kv_store;
        }

        if let Some(blob_store) = blob_store {
            self.database.blob_store = blob_store;
        }

        if let Some(address) = address {
            self.server.address = address.to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.server.address, "[::1]:9313");
        assert_eq!(config.database.backend, "sqlite");
        assert_eq!(config.database.kv_store, PathBuf::from("kv_store.db"));
        assert_eq!(config.database.blob_store, PathBuf::from("blob_store.db"));
    }

    #[test]
    fn test_config_from_file() -> Result<(), Box<dyn std::error::Error>> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(
            temp_file,
            r#"
[server]
address = "127.0.0.1:8080"

[database]
backend = "sqlite"
kv_store = "custom_kv.db"
blob_store = "custom_blob.db"

[logging]
level = "debug"

[performance]
"#
        )?;

        let config = Config::from_file(temp_file.path())?;
        assert_eq!(config.server.address, "127.0.0.1:8080");
        assert_eq!(config.database.backend, "sqlite");
        assert_eq!(config.database.kv_store, PathBuf::from("custom_kv.db"));
        assert_eq!(config.database.blob_store, PathBuf::from("custom_blob.db"));
        assert_eq!(config.logging.level, "debug");

        Ok(())
    }

    #[test]
    fn test_invalid_address() {
        let mut config = Config::default();
        config.server.address = "invalid-address".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_same_store_paths() {
        let mut config = Config::default();
        config.database.kv_store = PathBuf::from("same.db");
        config.database.blob_store = PathBuf::from("same.db");
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_apply_cli_overrides() {
        let mut config = Config::default();

        config.apply_cli_overrides(
            Some(ConfigBackend::Sqlite),
            Some(PathBuf::from("cli_kv.db")),
            Some(PathBuf::from("cli_blob.db")),
            Some("127.0.0.1:9000".parse().unwrap()),
        );

        assert_eq!(config.database.backend, "sqlite");
        assert_eq!(config.database.kv_store, PathBuf::from("cli_kv.db"));
        assert_eq!(config.database.blob_store, PathBuf::from("cli_blob.db"));
        assert_eq!(config.server.address, "127.0.0.1:9000");
    }
}
