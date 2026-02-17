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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct MongoBootstrapConfig {
    /// MongoDB connection string
    pub connection_string: String,

    /// Database name
    pub database: String,

    /// Collections to bootstrap
    #[serde(default)]
    pub collections: Vec<String>,

    /// Batch size for cursor iteration (default: 1000)
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
}

fn default_batch_size() -> u32 {
    1000
}

impl MongoBootstrapConfig {
    /// Validate the configuration and return an error if invalid.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.connection_string.is_empty() {
            return Err(anyhow::anyhow!(
                "Validation error: connection_string cannot be empty. \
                 Please specify the MongoDB connection string"
            ));
        }

        if self.database.is_empty() {
            return Err(anyhow::anyhow!(
                "Validation error: database cannot be empty. \
                 Please specify the MongoDB database name"
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
            "connection_string": "mongodb://localhost:27017",
            "database": "test",
            "collections": ["users"]
        }"#;

        let config: MongoBootstrapConfig = serde_json::from_str(config_json).unwrap();
        assert_eq!(config.batch_size, 1000);
    }
}
