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

//! MongoDB bootstrap provider implementation.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use drasi_core::models::{Element, ElementMetadata, ElementReference, SourceChange};
use drasi_lib::bootstrap::{BootstrapContext, BootstrapProvider, BootstrapRequest};
use futures::stream::TryStreamExt;
use log::{debug, error, info, warn};
use mongodb::{options::ClientOptions, Client};

use std::sync::Arc;
use tokio::sync::OnceCell;

use crate::config::MongoBootstrapConfig;
use crate::conversion::{bson_to_element_value, extract_element_id};

/// MongoDB bootstrap provider
pub struct MongoBootstrapProvider {
    config: MongoBootstrapConfig,
    client: OnceCell<Client>,
}

impl MongoBootstrapProvider {
    /// Create a new MongoDB bootstrap provider
    pub fn new(config: MongoBootstrapConfig) -> Self {
        Self {
            config,
            client: OnceCell::new(),
        }
    }

    /// Create a builder for MongoBootstrapProvider
    pub fn builder() -> MongoBootstrapProviderBuilder {
        MongoBootstrapProviderBuilder::new()
    }
}

impl Default for MongoBootstrapProvider {
    fn default() -> Self {
        Self::new(MongoBootstrapConfig::default())
    }
}

/// Builder for MongoBootstrapProvider
pub struct MongoBootstrapProviderBuilder {
    config: MongoBootstrapConfig,
}

impl MongoBootstrapProviderBuilder {
    pub fn new() -> Self {
        Self {
            config: MongoBootstrapConfig::default(),
        }
    }

    pub fn with_connection_string(mut self, connection_string: impl Into<String>) -> Self {
        self.config.connection_string = connection_string.into();
        self
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.config.database = database.into();
        self
    }

    pub fn with_collection(mut self, collection: impl Into<String>) -> Self {
        self.config.collections.push(collection.into());
        self
    }

    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.config.batch_size = batch_size;
        self
    }

    pub fn build(self) -> MongoBootstrapProvider {
        MongoBootstrapProvider::new(self.config)
    }
}

impl Default for MongoBootstrapProviderBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BootstrapProvider for MongoBootstrapProvider {
    async fn bootstrap(
        &self,
        request: BootstrapRequest,
        context: &BootstrapContext,
        event_tx: drasi_lib::channels::BootstrapEventSender,
        settings: Option<&drasi_lib::config::SourceSubscriptionSettings>,
    ) -> Result<usize> {
        info!(
            "Starting bootstrap for query {} from source {} (nodes: {:?}, relations: {:?})",
            request.query_id, context.source_id, request.node_labels, request.relation_labels
        );

        if let Some(s) = settings {
            info!(
                "Bootstrap for query {} (enable_bootstrap: {}, nodes: {:?}, relations: {:?})",
                s.query_id, s.enable_bootstrap, s.nodes, s.relations
            );
        }

        let client = self
            .client
            .get_or_try_init(|| async {
                let mut client_options =
                    ClientOptions::parse(&self.config.connection_string).await?;
                client_options.app_name = Some("drasi-bootstrap-mongodb".to_string());
                Client::with_options(client_options)
            })
            .await?;

        let db = client.database(&self.config.database);

        // Filter collections based on request labels
        // If request.node_labels is empty, we do nothing (scan 0 collections) per requirements.
        // If config.collections is set, we only scan those that are ALSO in request.node_labels.
        // Actually, requirement says "Only bootstraps collections matching requested node labels".
        // It implies the source of truth for "what exists" is the DB, but we filter by request.

        let resolved_collections = resolve_collections_to_scan(&self.config.collections, &request.node_labels);
        if resolved_collections.is_empty() {
            return Ok(0);
        }

        // Validate collections exist
        let existing_collections: std::collections::HashSet<String> = db
            .list_collection_names(None)
            .await?
            .into_iter()
            .collect();
            
        let mut collections_to_scan = Vec::new();
        for collection_name in resolved_collections {
            if existing_collections.contains(&collection_name) {
                collections_to_scan.push(collection_name);
            } else {
                warn!("Collection '{collection_name}' does not exist, skipping");
                // Log warning similar to Postgres "Table does not exist"
            }
        }

        if collections_to_scan.is_empty() {
            warn!("No valid collections found to scan");
            return Ok(0);
        }

        info!("Resolved {} verified collections to scan: {:?}", collections_to_scan.len(), collections_to_scan);

        let mut total_count = 0;
        let mut batch = Vec::with_capacity(self.config.batch_size as usize);

        for collection_name in collections_to_scan {
            let collection = db.collection::<bson::Document>(&collection_name);
            debug!("Scanning collection: {collection_name}");

            let find_options = mongodb::options::FindOptions::builder()
                .batch_size(self.config.batch_size)
                .build();

            let mut cursor = collection.find(None, find_options).await?;

            while let Some(doc) = cursor.try_next().await? {
                // Convert doc to BootstrapEvent
                // Fail fast on error to match Postgres behavior
                let event = self.process_document(&doc, &collection_name, context)?;

                batch.push(event);

                if batch.len() >= self.config.batch_size as usize {
                    self.send_batch(&mut batch, event_tx.clone()).await?;
                    total_count += self.config.batch_size as usize;
                }
            }
        }

        // Send remaining events
        if !batch.is_empty() {
            total_count += batch.len();
            self.send_batch(&mut batch, event_tx).await?;
        }

        info!(
            "Completed bootstrap for query {}: sent {} elements",
            request.query_id, total_count
        );
        Ok(total_count)
    }
}

impl MongoBootstrapProvider {
    fn process_document(
        &self,
        doc: &bson::Document,
        collection_name: &str,
        context: &BootstrapContext,
    ) -> Result<drasi_lib::channels::BootstrapEvent> {
        let element_id = extract_element_id(doc, collection_name)?;

        let mut properties = drasi_core::models::ElementPropertyMap::new();

        // Map top-level fields, excluding _id (it's in the element_id)
        for (key, value) in doc {
            if key != "_id" {
                properties.insert(key, bson_to_element_value(value));
            }
        }

        let metadata = ElementMetadata {
            reference: ElementReference::new(&context.source_id, &element_id),
            labels: Arc::from(vec![Arc::from(collection_name)]),
            effective_from: 0,
        };

        let element = Element::Node {
            metadata,
            properties,
        };

        let change = SourceChange::Insert { element };

        Ok(self.create_event(change, context))
    }

    /// Create a BootstrapEvent from a SourceChange
    fn create_event(
        &self,
        change: SourceChange,
        context: &BootstrapContext,
    ) -> drasi_lib::channels::BootstrapEvent {
        drasi_lib::channels::BootstrapEvent {
            source_id: context.source_id.clone(),
            change,
            timestamp: chrono::Utc::now(),
            sequence: context.next_sequence(),
        }
    }

    async fn send_batch(
        &self,
        batch: &mut Vec<drasi_lib::channels::BootstrapEvent>,
        event_tx: drasi_lib::channels::BootstrapEventSender,
    ) -> Result<()> {
        for event in batch.drain(..) {
            event_tx
                .send(event)
                .await
                .map_err(|e| anyhow!("Failed to send event: {e}"))?;
        }
        Ok(())
    }
}

fn resolve_collections_to_scan(configured: &[String], requested: &[String]) -> Vec<String> {
    if requested.is_empty() {
        warn!("No node labels requested, skipping bootstrap");
        return Vec::new();
    }

    // We use the intersection of configured collections (if any) and requested labels
    requested
        .iter()
        .map(|s| s.to_string())
        .filter(|label| {
            if configured.is_empty() {
                true
            } else {
                configured.contains(label)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_collections_empty_request() {
        let configured = vec!["users".to_string()];
        let requested = vec![];
        let resolved = resolve_collections_to_scan(&configured, &requested);
        assert!(resolved.is_empty());
    }

    #[test]
    fn test_resolve_collections_strict_config() {
        let configured = vec!["users".to_string(), "orders".to_string()];
        let requested = vec!["users".to_string(), "products".to_string()];
        let resolved = resolve_collections_to_scan(&configured, &requested);
        assert_eq!(resolved, vec!["users".to_string()]);
    }

    #[test]
    fn test_resolve_collections_open_config() {
        let configured = vec![];
        let requested = vec!["users".to_string(), "products".to_string()];
        let resolved = resolve_collections_to_scan(&configured, &requested);
        assert_eq!(resolved, vec!["users".to_string(), "products".to_string()]);
    }
}
