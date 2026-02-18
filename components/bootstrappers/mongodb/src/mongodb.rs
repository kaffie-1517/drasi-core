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
use log::{debug, info, warn};
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

    /// Access the configuration (for testing/inspection).
    pub fn config(&self) -> &MongoBootstrapConfig {
        &self.config
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
        _settings: Option<&drasi_lib::config::SourceSubscriptionSettings>,
    ) -> Result<usize> {
        info!(
            "Starting MongoDB bootstrap for query '{}' with {} node labels and {} relation labels",
            request.query_id,
            request.node_labels.len(),
            request.relation_labels.len()
        );

        let client = self
            .client
            .get_or_try_init(|| async {
                let mut client_options =
                    ClientOptions::parse(&self.config.connection_string).await?;
                client_options.app_name = Some("drasi-bootstrap-mongodb".to_string());
                Client::with_options(client_options)
            })
            .await?;

        // Extract the database name from the connection string URI path.
        let database_name = self.config.database().ok_or_else(|| {
            anyhow!("connection_string must include a database name in the URI path \
                     (e.g., mongodb://host:27017/mydb)")
        })?;

        let db = client.database(&database_name);

        // Determine which collections to scan: the intersection of configured
        // collections (if any) and the node labels requested by the query.
        // If request.node_labels is empty, no collections are scanned.
        let resolved_collections =
            resolve_collections_to_scan(&self.config.collections, &request.node_labels);
        if resolved_collections.is_empty() {
            return Ok(0);
        }

        // Validate that each resolved collection actually exists in the database.
        let existing_collections: std::collections::HashSet<String> =
            db.list_collection_names(None).await?.into_iter().collect();

        let mut collections_to_scan = Vec::new();
        for collection_name in resolved_collections {
            if existing_collections.contains(&collection_name) {
                collections_to_scan.push(collection_name);
            } else {
                warn!(
                    "Collection '{collection_name}' does not exist in database '{database_name}', skipping",
                );
            }
        }

        if collections_to_scan.is_empty() {
            warn!("No valid collections found to scan");
            return Ok(0);
        }

        info!(
            "Resolved {} verified collections to scan: {:?}",
            collections_to_scan.len(),
            collections_to_scan
        );

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
                // Filter BEFORE creating Element (saves allocation) — as per Developer Guide.
                // The document's label is its collection name; skip if it
                // doesn't match the requested labels.
                if !matches_labels(&[collection_name.clone()], &request.node_labels) {
                    continue;
                }

                let event = self.process_document(&doc, &collection_name, context)?;
                batch.push(event);

                if batch.len() >= self.config.batch_size as usize {
                    let flushed = batch.len();
                    self.send_batch(&mut batch, event_tx.clone()).await?;
                    total_count += flushed;
                }
            }
        }

        // Send any remaining events that did not fill a complete batch.
        if !batch.is_empty() {
            total_count += batch.len();
            self.send_batch(&mut batch, event_tx).await?;
        }

        info!(
            "Completed MongoDB bootstrap for query '{}': sent {} elements",
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

        // Map all top-level fields except _id (already encoded in element_id).
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
                .map_err(|e| anyhow!("Failed to send bootstrap event to channel: {e}"))?;
        }
        Ok(())
    }
}

/// Resolve which collections to scan given the configured allow-list and the
/// labels requested by the query.
///
/// - If `requested` is empty → scan nothing (return empty).
/// - If `configured` is empty → scan everything in `requested`.
/// - Otherwise → scan the intersection of `configured` and `requested`.
fn resolve_collections_to_scan(configured: &[String], requested: &[String]) -> Vec<String> {
    if requested.is_empty() {
        warn!("No node labels requested, skipping bootstrap");
        return Vec::new();
    }

    requested
        .iter()
        .filter(|label| configured.is_empty() || configured.contains(label))
        .map(|s| s.to_string())
        .collect()
}

/// Per-document label filter as recommended by the Developer Guide.
///
/// Returns `true` if the element should be included:
/// - If no labels are requested → include all.
/// - Otherwise → include if any element label is in the requested set.
fn matches_labels(element_labels: &[String], requested_labels: &[String]) -> bool {
    if requested_labels.is_empty() {
        return true;
    }
    element_labels
        .iter()
        .any(|label| requested_labels.contains(label))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_collections_empty_request() {
        let configured = vec!["users".to_string()];
        let requested: Vec<String> = vec![];
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
        let configured: Vec<String> = vec![];
        let requested = vec!["users".to_string(), "products".to_string()];
        let resolved = resolve_collections_to_scan(&configured, &requested);
        assert_eq!(
            resolved,
            vec!["users".to_string(), "products".to_string()]
        );
    }

    #[test]
    fn test_resolve_collections_fully_disjoint() {
        // configured and requested have no overlap → nothing to scan
        let configured = vec!["users".to_string(), "orders".to_string()];
        let requested = vec!["products".to_string(), "logs".to_string()];
        let resolved = resolve_collections_to_scan(&configured, &requested);
        assert!(resolved.is_empty());
    }

    #[test]
    fn test_resolve_collections_partial_overlap() {
        let configured = vec!["users".to_string(), "orders".to_string()];
        let requested = vec![
            "users".to_string(),
            "orders".to_string(),
            "sessions".to_string(),
        ];
        let resolved = resolve_collections_to_scan(&configured, &requested);
        assert_eq!(
            resolved,
            vec!["users".to_string(), "orders".to_string()]
        );
    }

    /// Verifies that sequence numbers produced by `process_document` are
    /// strictly monotonically increasing when processing multiple documents —
    /// exercising the real code path (BSON → Element → BootstrapEvent).
    #[test]
    fn test_sequence_numbers_are_monotonic() {
        let context = BootstrapContext::new_minimal(
            "test-server".to_string(),
            "test-source".to_string(),
        );

        let provider = MongoBootstrapProvider::default();

        // Simulate processing multiple realistic BSON documents
        let docs: Vec<bson::Document> = (0..10)
            .map(|i| {
                bson::doc! {
                    "_id": bson::oid::ObjectId::new(),
                    "name": format!("user_{i}"),
                    "index": i as i32,
                }
            })
            .collect();

        let mut sequences = Vec::new();
        for doc in &docs {
            let event = provider
                .process_document(doc, "users", &context)
                .expect("process_document should succeed");
            sequences.push(event.sequence);
        }

        // Every sequence number must be strictly greater than the previous
        for i in 1..sequences.len() {
            assert!(
                sequences[i] > sequences[i - 1],
                "Sequence numbers must be strictly increasing: seq[{}]={} is not > seq[{}]={}",
                i,
                sequences[i],
                i - 1,
                sequences[i - 1]
            );
        }

        // First sequence should start at 0 (AtomicU64 initialized to 0, fetch_add returns previous value)
        assert_eq!(sequences[0], 0, "First sequence number should be 0");
        assert_eq!(sequences[9], 9, "Last sequence number should be 9");
    }

    /// Verifies that `process_document` produces correct BootstrapEvents with the
    /// expected source_id, SourceChange::Insert variant, and element metadata.
    #[test]
    fn test_process_document_creates_correct_event() {
        let context = BootstrapContext::new_minimal(
            "test-server".to_string(),
            "test-source".to_string(),
        );

        let provider = MongoBootstrapProvider::default();

        let doc = bson::doc! {
            "_id": bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap(),
            "name": "Alice",
            "age": 30_i32,
        };

        let event = provider
            .process_document(&doc, "users", &context)
            .expect("process_document should succeed");

        // Event should have the correct source_id
        assert_eq!(event.source_id, "test-source");

        // Event change should be Insert
        match &event.change {
            SourceChange::Insert { element } => {
                match element {
                    Element::Node {
                        metadata,
                        properties,
                    } => {
                        // Element ID = "users:507f1f77bcf86cd799439011"
                        assert_eq!(
                            metadata.reference.element_id.as_ref(),
                            "users:507f1f77bcf86cd799439011"
                        );
                        assert_eq!(metadata.reference.source_id.as_ref(), "test-source");
                        assert_eq!(metadata.effective_from, 0);

                        // Labels = ["users"]
                        assert_eq!(metadata.labels.len(), 1);
                        assert_eq!(metadata.labels[0].as_ref(), "users");

                        // Properties should NOT contain _id, but should contain name and age
                        assert!(properties.get("_id").is_none());
                        assert_eq!(
                            properties.get("name").unwrap(),
                            &drasi_core::models::ElementValue::String(Arc::from("Alice"))
                        );
                        assert_eq!(
                            properties.get("age").unwrap(),
                            &drasi_core::models::ElementValue::Integer(30)
                        );
                    }
                    _ => panic!("Expected Element::Node"),
                }
            }
            _ => panic!("Expected SourceChange::Insert"),
        }
    }

    /// Verifies that events can be successfully sent through an mpsc channel,
    /// matching the Developer Guide's recommended test pattern.
    #[tokio::test]
    async fn test_send_batch_delivers_events() {
        let context = BootstrapContext::new_minimal(
            "test-server".to_string(),
            "test-source".to_string(),
        );

        let provider = MongoBootstrapProvider::default();
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        // Create a batch of events
        let mut batch = Vec::new();
        for i in 0..5 {
            let element = Element::Node {
                metadata: ElementMetadata {
                    reference: ElementReference::new("test-source", &format!("col:{i}")),
                    labels: Arc::from(vec![Arc::from("col")]),
                    effective_from: 0,
                },
                properties: drasi_core::models::ElementPropertyMap::new(),
            };
            let change = SourceChange::Insert { element };
            batch.push(provider.create_event(change, &context));
        }

        // Send the batch
        provider
            .send_batch(&mut batch, tx)
            .await
            .expect("send_batch should succeed");

        // Batch should be drained
        assert!(batch.is_empty());

        // All events should be received
        let mut received = 0;
        while let Ok(event) = rx.try_recv() {
            assert_eq!(event.source_id, "test-source");
            assert!(matches!(event.change, SourceChange::Insert { .. }));
            received += 1;
        }
        assert_eq!(received, 5);
    }
}