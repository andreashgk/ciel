use std::env;
use std::env::current_dir;
use std::io;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use thiserror::Error;
use tokio::fs;
use toml::Value;
use toml::map::Map;
use tracing::debug;

#[derive(Clone)]
pub struct Config {
    values: Arc<Value>,
    scope: String,
}

impl Config {
    pub async fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        debug!(
            path = path.to_string_lossy().as_ref(),
            "reading config from file"
        );

        let file = fs::read_to_string(path).await?;
        let values =
            toml::from_str(&file).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

        let current_dir = current_dir()?;
        let values = resolve(path.parent().unwrap_or(current_dir.as_path()), values).await?;

        Ok(Self {
            values: Arc::new(values),
            scope: String::new(),
        })
    }

    /// Returns a copy of the config restricted within a scope.
    ///
    /// All subsequent read operations on this copy will be performed as if the scope key is the
    /// root.
    pub fn scoped(&self, scope: &str) -> Self {
        let scope = if self.scope.is_empty() {
            scope.to_string()
        } else {
            format!("{}.{}", self.scope.as_str(), scope.trim_matches('.'))
        };
        Self {
            scope,
            values: self.values.clone(),
        }
    }

    pub fn read<'a, V>(&self, path: &str) -> Result<V, ConfigError>
    where
        V: Deserialize<'a>,
    {
        self.read_optional(path).and_then(|val| {
            val.ok_or_else(|| ConfigError::KeyNotFound {
                path: format!("{}.{}", self.scope, path),
            })
        })
    }

    pub fn read_optional<'a, V>(&self, path: &str) -> Result<Option<V>, ConfigError>
    where
        V: Deserialize<'a>,
    {
        let Some(value) = self.read_value(path) else {
            return Ok(None);
        };
        value
            .to_owned()
            .try_into()
            .map_err(|err| ConfigError::TypeMismatch {
                path: format!("{}.{}", self.scope, path),
                why: err.to_string(),
            })
    }

    pub fn read_value(&self, path: &str) -> Option<&Value> {
        let path_iter = self
            .scope
            .split('.')
            .chain(path.split('.'))
            // Either the scope or path can be empty which would result in an extra empty element.
            // A simply way to prevent this is just filtering those.
            .filter(|part| !part.is_empty());

        let mut value = self.values.as_ref();

        for subpath in path_iter {
            match value {
                Value::Array(values) => {
                    let index = subpath.parse::<usize>().ok()?;
                    value = values.get(index)?;
                }
                Value::Table(map) => {
                    value = map.get(subpath)?;
                }
                _ => {
                    return None;
                }
            }
        }

        Some(value)
    }
}

/// Possible errors that can occur when reading the config.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The target value at the provided path (or a parent) did not exist.
    #[error("missing key: {path}")]
    KeyNotFound { path: String },
    /// Expected a value to be of one type, but it was of another type.
    #[error("type mismatch: {path}: {why}")]
    TypeMismatch { path: String, why: String },
    /// Any other error.
    #[error("{0}")]
    Other(String),
}

// TODO: provide path diagnostics
// TODO: manually use parsed values
async fn resolve(relative_path: &Path, value: toml::Value) -> io::Result<toml::Value> {
    match value {
        Value::Array(values) => {
            let mut new_values = Vec::with_capacity(values.len());
            // TODO: do in parallel
            for value in values {
                new_values.push(Box::pin(resolve(relative_path, value)).await?);
            }
            Ok(Value::Array(new_values))
        }
        Value::Table(map) => {
            if map.len() == 1 {
                match map
                    .iter()
                    .next()
                    .map(|(k, v)| (k.as_str(), v.as_str().ok_or(v)))
                {
                    Some(("from-file", Ok(val))) => {
                        let path = relative_path.join(val);
                        debug!(
                            path.relative = val,
                            path.resolved = path.to_string_lossy().as_ref(),
                            "reading field from file"
                        );

                        // TODO: it could be cool to also support toml files here
                        let file = fs::read_to_string(path).await?;
                        return Ok(Value::String(file));
                    }
                    Some(("from-env", Ok(val))) => {
                        debug!(key = val, "reading field from environment");
                        let var = env::var(val).map_err(|err| match &err {
                            env::VarError::NotPresent => {
                                io::Error::new(io::ErrorKind::NotFound, err)
                            }
                            env::VarError::NotUnicode(_os_string) => {
                                io::Error::new(io::ErrorKind::InvalidData, err)
                            }
                        })?;
                        return Ok(Value::String(var));
                    }
                    Some((k @ ("from-file" | "from-env"), Err(val))) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "value of `{k}` should be of type string, got {}",
                                val.type_str()
                            ),
                        ));
                    }
                    _ => {}
                }
            }

            let mut new_map = Map::with_capacity(map.len());
            // TODO: do in parallel
            for (key, value) in map {
                new_map.insert(key, Box::pin(resolve(relative_path, value)).await?);
            }
            Ok(Value::Table(new_map))
        }
        val => Ok(val),
    }
}
