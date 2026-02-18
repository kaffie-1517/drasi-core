// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configuration types for the MongoDB bootstrap provider.
//!
//! These types are defined locally to keep this component independent
//! and self-contained.

use serde::{Deserialize, Serialize};

/// MongoDB bootstrap provider configuration
#[derive(Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct MongoBootstrapConfig {
    /// MongoDB connection string
    pub connection_string: String,

    /// Collections to bootstrap
    #[serde(default)]
    pub collections: Vec<String>,

    /// Batch size for cursor iteration
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
}

fn default_batch_size() -> u32 {
    1000
}

/// Redact credentials from a MongoDB connection string.
fn redact_connection_string(conn_str: &str) -> String {
    if let Some(at_pos) = conn_str.find('@') {
        if let Some(scheme_end) = conn_str.find("://") {
            let scheme = &conn_str[..scheme_end + 3];
            let after_at = &conn_str[at_pos..];
            return format!("{scheme}***:***{after_at}");
        }
    }
    conn_str.to_string()
}

/// Extract the database name from a MongoDB connection string URI path.
pub(crate) fn parse_database_from_uri(conn_str: &str) -> Option<String> {

    let after_scheme = conn_str
        .find("://")
        .map(|i| &conn_str[i + 3..])
        .unwrap_or(conn_str);

    let after_userinfo = after_scheme
        .find('@')
        .map(|i| &after_scheme[i + 1..])
        .unwrap_or(after_scheme);

    if let Some(slash_pos) = after_userinfo.find('/') {
        let path = &after_userinfo[slash_pos + 1..];
        let db_name = path.split('?').next().unwrap_or("");

        if !db_name.is_empty() {
            return Some(db_name.to_string());
        }
    }

    None
}

impl std::fmt::Debug for MongoBootstrapConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoBootstrapConfig")
            .field("connection_string", &redact_connection_string(&self.connection_string))
            .field("database", &parse_database_from_uri(&self.connection_string).unwrap_or_default())
            .field("collections", &self.collections)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl MongoBootstrapConfig {
    /// Extract the database name from the connection string.
    pub fn database(&self) -> Option<String> {
        parse_database_from_uri(&self.connection_string)
    }

    /// Validate the configuration and return an error if invalid.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.connection_string.is_empty() {
            return Err(anyhow::anyhow!(
                "Validation error: connection_string cannot be empty. \
                 Please specify the MongoDB connection string"
            ));
        }

        if self.database().is_none() {
            return Err(anyhow::anyhow!(
                "Validation error: connection_string must include a database name \
                 in the URI path (e.g., mongodb://host:27017/mydb)"
            ));
        }

        if self.batch_size == 0 {
            return Err(anyhow::anyhow!(
                "Validation error: batch_size cannot be 0. \
                 Please specify a valid batch size (> 0)"
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_batch_size() {
        let config_json = r#"{
            "connection_string": "mongodb://localhost:27017/testdb",
            "collections": ["users"]
        }"#;

        let config: MongoBootstrapConfig = serde_json::from_str(config_json)
            .expect("valid JSON should deserialize");
        assert_eq!(config.batch_size, 1000);
    }

    // Database parsing 

    #[test]
    fn test_database_from_simple_uri() {
        let config = MongoBootstrapConfig {
            connection_string: "mongodb://localhost:27017/inventory".to_string(),
            ..Default::default()
        };
        assert_eq!(config.database(), Some("inventory".to_string()));
    }

    #[test]
    fn test_database_from_uri_with_credentials() {
        let config = MongoBootstrapConfig {
            connection_string: "mongodb://user:pass@host:27017/mydb".to_string(),
            ..Default::default()
        };
        assert_eq!(config.database(), Some("mydb".to_string()));
    }

    #[test]
    fn test_database_from_uri_with_query_string() {
        let config = MongoBootstrapConfig {
            connection_string: "mongodb://host:27017/mydb?retryWrites=true&w=majority".to_string(),
            ..Default::default()
        };
        assert_eq!(config.database(), Some("mydb".to_string()));
    }

    #[test]
    fn test_database_none_when_missing() {
        let config = MongoBootstrapConfig {
            connection_string: "mongodb://localhost:27017".to_string(),
            ..Default::default()
        };
        assert_eq!(config.database(), None);
    }

    #[test]
    fn test_database_none_for_trailing_slash() {
        let config = MongoBootstrapConfig {
            connection_string: "mongodb://localhost:27017/".to_string(),
            ..Default::default()
        };
        assert_eq!(config.database(), None);
    }

    // Validation

    #[test]
    fn test_validate_fails_without_database_in_uri() {
        let config = MongoBootstrapConfig {
            connection_string: "mongodb://localhost:27017".to_string(),
            ..Default::default()
        };
        let err = config.validate().expect_err("should fail without database in URI");
        assert!(err.to_string().contains("must include a database name"));
    }

    #[test]
    fn test_validate_passes_with_database_in_uri() {
        let config = MongoBootstrapConfig {
            connection_string: "mongodb://localhost:27017/mydb".to_string(),
            batch_size: 1000,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    //Debugging

    #[test]
    fn test_debug_redacts_credentials() {
        let config = MongoBootstrapConfig {
            connection_string: "mongodb://admin:s3cret@db.example.com:27017/mydb".to_string(),
            collections: vec!["users".to_string()],
            batch_size: 1000,
        };

        let debug_output = format!("{:?}", config);

        assert!(!debug_output.contains("admin"), "Username leaked in Debug output");
        assert!(!debug_output.contains("s3cret"), "Password leaked in Debug output");
        assert!(debug_output.contains("***:***"), "Redacted placeholder missing");
        assert!(debug_output.contains("db.example.com"), "Host should be visible");
        assert!(debug_output.contains("mydb"), "Database should be visible");
    }

    #[test]
    fn test_debug_no_credentials_unchanged() {
        let config = MongoBootstrapConfig {
            connection_string: "mongodb://localhost:27017/testdb".to_string(),
            collections: vec![],
            batch_size: 1000,
        };

        let debug_output = format!("{:?}", config);
        assert!(debug_output.contains("testdb"), "Database should be visible in debug");
    }
}
