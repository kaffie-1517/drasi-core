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

//! Integration tests for the MongoDB bootstrap provider using testcontainers.
//!
//! These tests require Docker and are marked `#[ignore]` by default.
//!
//! Run with:
//! ```sh
//! cargo test -p drasi-bootstrap-mongodb --test mongo_integration -- --ignored
//! ```

use std::sync::Arc;

use bson::doc;
use drasi_core::models::{Element, ElementValue, SourceChange};
use drasi_lib::bootstrap::{BootstrapContext, BootstrapProvider, BootstrapRequest};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mongo::Mongo;

use drasi_bootstrap_mongodb::MongoBootstrapProvider;

/// Start a MongoDB replica set container and return (container, connection_url).
async fn start_mongo_replica_set(
) -> (testcontainers::ContainerAsync<Mongo>, String) {
    let container = Mongo::repl_set()
        .start()
        .await
        .expect("Failed to start MongoDB container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("Failed to get port");

    let url = format!("mongodb://{host}:{port}/testdb?directConnection=true");

    (container, url)
}

/// Create a standard BootstrapRequest for testing.
fn make_request(node_labels: Vec<&str>) -> BootstrapRequest {
    BootstrapRequest {
        query_id: "test-query".to_string(),
        node_labels: node_labels.into_iter().map(|s| s.to_string()).collect(),
        relation_labels: vec![],
        request_id: "test-request".to_string(),
    }
}

/// Create a BootstrapContext.
fn make_context() -> BootstrapContext {
    BootstrapContext::new_minimal("test-server".to_string(), "test-source".to_string())
}

/// End-to-end test: insert documents, run bootstrap, verify events.
#[tokio::test]
#[ignore]
async fn test_bootstrap_with_real_mongodb() {
    let (_container, url) = start_mongo_replica_set().await;

    // Seed the database
    let client = mongodb::Client::with_uri_str(&url)
        .await
        .expect("Failed to connect to MongoDB");
    let db = client.database("testdb");
    let coll = db.collection::<bson::Document>("users");

    coll.insert_many(
        vec![
            doc! { "name": "Alice", "age": 30_i32 },
            doc! { "name": "Bob", "age": 25_i32 },
            doc! { "name": "Charlie", "age": 35_i32 },
        ],
        None,
    )
    .await
    .expect("Failed to insert seed data");

    // Create bootstrap provider
    let provider = MongoBootstrapProvider::builder()
        .with_connection_string(&url)
        .build();

    let context = make_context();
    let request = make_request(vec!["users"]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let count = provider
        .bootstrap(request, &context, tx, None)
        .await
        .expect("Bootstrap should succeed");

    assert_eq!(count, 3, "Should bootstrap exactly 3 documents");

    // Collect and verify events
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }

    assert_eq!(events.len(), 3);

    // All events should be Insert with source_id "test-source"
    for event in &events {
        assert_eq!(event.source_id, "test-source");
        assert!(matches!(event.change, SourceChange::Insert { .. }));
    }

    // Verify sequence numbers are monotonically increasing
    for i in 1..events.len() {
        assert!(
            events[i].sequence > events[i - 1].sequence,
            "Sequence numbers must be strictly increasing"
        );
    }

    // Verify element IDs start with "users:"
    for event in &events {
        if let SourceChange::Insert { element } = &event.change {
            if let Element::Node { metadata, .. } = element {
                assert!(
                    metadata.reference.element_id.starts_with("users:"),
                    "Element ID should start with 'users:'"
                );
                assert_eq!(metadata.labels.len(), 1);
                assert_eq!(metadata.labels[0].as_ref(), "users");
                assert_eq!(metadata.effective_from, 0);
            }
        }
    }

    // Verify property values by collecting names
    let mut names: Vec<String> = events
        .iter()
        .filter_map(|e| {
            if let SourceChange::Insert { element } = &e.change {
                if let Element::Node { properties, .. } = element {
                    if let Some(ElementValue::String(s)) = properties.get("name") {
                        return Some(s.to_string());
                    }
                }
            }
            None
        })
        .collect();
    names.sort();

    assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
}

/// Test that bootstrapping an empty collection returns zero events.
#[tokio::test]
#[ignore]
async fn test_bootstrap_empty_collection() {
    let (_container, url) = start_mongo_replica_set().await;

    // Create the collection without inserting any documents
    let client = mongodb::Client::with_uri_str(&url)
        .await
        .expect("Failed to connect to MongoDB");
    let db = client.database("testdb");
    db.create_collection("empty_col", None)
        .await
        .expect("Failed to create collection");

    let provider = MongoBootstrapProvider::builder()
        .with_connection_string(&url)
        .build();

    let context = make_context();
    let request = make_request(vec!["empty_col"]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let count = provider
        .bootstrap(request, &context, tx, None)
        .await
        .expect("Bootstrap should succeed");

    assert_eq!(count, 0, "Empty collection should produce 0 events");
    assert!(rx.try_recv().is_err(), "No events should be received");
}

/// Test label filtering: only collections matching requested labels are bootstrapped.
#[tokio::test]
#[ignore]
async fn test_bootstrap_label_filtering() {
    let (_container, url) = start_mongo_replica_set().await;

    // Seed two collections
    let client = mongodb::Client::with_uri_str(&url)
        .await
        .expect("Failed to connect to MongoDB");
    let db = client.database("testdb");

    let users = db.collection::<bson::Document>("users");
    users
        .insert_many(
            vec![
                doc! { "name": "Alice" },
                doc! { "name": "Bob" },
            ],
            None,
        )
        .await
        .expect("Failed to insert users");

    let orders = db.collection::<bson::Document>("orders");
    orders
        .insert_many(
            vec![
                doc! { "product": "Widget", "qty": 10_i32 },
                doc! { "product": "Gadget", "qty": 5_i32 },
                doc! { "product": "Thingamajig", "qty": 1_i32 },
            ],
            None,
        )
        .await
        .expect("Failed to insert orders");

    let provider = MongoBootstrapProvider::builder()
        .with_connection_string(&url)
        .build();

    // Request only "orders" — should NOT bootstrap "users"
    let context = make_context();
    let request = make_request(vec!["orders"]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let count = provider
        .bootstrap(request, &context, tx, None)
        .await
        .expect("Bootstrap should succeed");

    assert_eq!(count, 3, "Should only bootstrap 3 orders, not 2 users");

    // Verify all events are from "orders" collection
    while let Ok(event) = rx.try_recv() {
        if let SourceChange::Insert { element } = &event.change {
            if let Element::Node { metadata, .. } = element {
                assert!(
                    metadata.reference.element_id.starts_with("orders:"),
                    "All element IDs should be from 'orders' collection"
                );
                assert_eq!(metadata.labels[0].as_ref(), "orders");
            }
        }
    }
}

/// Test that BSON type conversions round-trip correctly through a real MongoDB.
#[tokio::test]
#[ignore]
async fn test_bootstrap_bson_type_conversion() {
    let (_container, url) = start_mongo_replica_set().await;

    let client = mongodb::Client::with_uri_str(&url)
        .await
        .expect("Failed to connect to MongoDB");
    let db = client.database("testdb");
    let coll = db.collection::<bson::Document>("typed_data");

    let oid = bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011")
        .expect("valid ObjectId hex string");
    let dt = bson::DateTime::from_millis(1705312800000); // 2024-01-15T10:00:00Z

    coll.insert_one(
        doc! {
            "_id": oid,
            "string_field": "hello",
            "int32_field": 42_i32,
            "int64_field": 1234567890123_i64,
            "double_field": 3.14,
            "bool_field": true,
            "null_field": bson::Bson::Null,
            "datetime_field": dt,
            "nested_doc": { "a": 1_i32, "b": "two" },
            "array_field": [1_i32, 2_i32, 3_i32],
        },
        None,
    )
    .await
    .expect("Failed to insert typed document");

    let provider = MongoBootstrapProvider::builder()
        .with_connection_string(&url)
        .build();

    let context = make_context();
    let request = make_request(vec!["typed_data"]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let count = provider
        .bootstrap(request, &context, tx, None)
        .await
        .expect("Bootstrap should succeed");

    assert_eq!(count, 1);

    let event = rx.try_recv().expect("Should receive one event");
    if let SourceChange::Insert { element } = &event.change {
        if let Element::Node {
            metadata,
            properties,
        } = element
        {
            // Element ID
            assert_eq!(
                metadata.reference.element_id.as_ref(),
                "typed_data:507f1f77bcf86cd799439011"
            );

            // _id excluded
            assert!(properties.get("_id").is_none());

            // String
            assert_eq!(
                properties.get("string_field").expect("'string_field' should exist"),
                &ElementValue::String(Arc::from("hello"))
            );

            // Int32 → Integer (as i64)
            assert_eq!(
                properties.get("int32_field").expect("'int32_field' should exist"),
                &ElementValue::Integer(42)
            );

            // Int64 → Integer
            assert_eq!(
                properties.get("int64_field").expect("'int64_field' should exist"),
                &ElementValue::Integer(1234567890123)
            );

            // Double → Float
            assert_eq!(
                properties.get("double_field").expect("'double_field' should exist"),
                &ElementValue::Float(ordered_float::OrderedFloat(3.14))
            );

            // Boolean
            assert_eq!(
                properties.get("bool_field").expect("'bool_field' should exist"),
                &ElementValue::Bool(true)
            );

            // Null
            assert_eq!(
                properties.get("null_field").expect("'null_field' should exist"),
                &ElementValue::Null
            );

            // DateTime → String (RFC 3339 with Z suffix)
            if let ElementValue::String(s) = properties.get("datetime_field").expect("'datetime_field' should exist") {
                assert!(
                    s.ends_with('Z'),
                    "DateTime should end with 'Z', got: {s}"
                );
                assert!(
                    s.contains("2024-01-15"),
                    "DateTime should contain date, got: {s}"
                );
            } else {
                panic!("datetime_field should be ElementValue::String");
            }

            // Nested Document → Object
            if let ElementValue::Object(obj) = properties.get("nested_doc").expect("'nested_doc' should exist") {
                assert_eq!(obj.get("a").expect("'a' should exist"), &ElementValue::Integer(1));
                assert_eq!(
                    obj.get("b").expect("'b' should exist"),
                    &ElementValue::String(Arc::from("two"))
                );
            } else {
                panic!("nested_doc should be ElementValue::Object");
            }

            // Array → List
            if let ElementValue::List(list) = properties.get("array_field").expect("'array_field' should exist") {
                assert_eq!(list.len(), 3);
                assert_eq!(list[0], ElementValue::Integer(1));
                assert_eq!(list[1], ElementValue::Integer(2));
                assert_eq!(list[2], ElementValue::Integer(3));
            } else {
                panic!("array_field should be ElementValue::List");
            }
        } else {
            panic!("Expected Element::Node");
        }
    } else {
        panic!("Expected SourceChange::Insert");
    }
}

/// Test that requesting labels for a non-existent collection succeeds with 0 events.
#[tokio::test]
#[ignore]
async fn test_bootstrap_nonexistent_collection() {
    let (_container, url) = start_mongo_replica_set().await;

    let provider = MongoBootstrapProvider::builder()
        .with_connection_string(&url)
        .build();

    let context = make_context();
    let request = make_request(vec!["nonexistent_collection"]);
    let (tx, _rx) = tokio::sync::mpsc::channel(100);

    let count = provider
        .bootstrap(request, &context, tx, None)
        .await
        .expect("Bootstrap should succeed even for nonexistent collection");

    assert_eq!(count, 0);
}

/// Test that a connection string with embedded credentials works correctly.
#[tokio::test]
#[ignore]
async fn test_bootstrap_with_credentials() {
    let container = Mongo::repl_set()
        .start()
        .await
        .expect("Failed to start MongoDB container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("Failed to get port");

    // Step 1: Connect without credentials to set up the database
    let admin_url = format!("mongodb://{host}:{port}/testdb?directConnection=true");
    let client = mongodb::Client::with_uri_str(&admin_url)
        .await
        .expect("Failed to connect to MongoDB");

    // Create a user on the testdb database. MongoDB authenticates against
    // the database specified in the URI path, so the user must exist there.
    let db = client.database("testdb");
    db.run_command(
            doc! {
                "createUser": "myuser",
                "pwd": "mypassword",
                "roles": [{ "role": "readWrite", "db": "testdb" }]
            },
            None,
        )
        .await
        .expect("Failed to create MongoDB user");

    // Seed test data
    let coll = db.collection::<bson::Document>("cred_test");

    coll.insert_many(
        vec![
            doc! { "name": "Alice" },
            doc! { "name": "Bob" },
        ],
        None,
    )
    .await
    .expect("Failed to insert seed data");

    // Step 2: Bootstrap using a credentialed connection string, as it would
    // be when sourced from a Kubernetes Secret or environment variable.
    let cred_url = format!(
        "mongodb://myuser:mypassword@{host}:{port}/testdb?directConnection=true"
    );

    let provider = MongoBootstrapProvider::builder()
        .with_connection_string(&cred_url)
        .build();

    let context = make_context();
    let request = make_request(vec!["cred_test"]);
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let count = provider
        .bootstrap(request, &context, tx, None)
        .await
        .expect("Bootstrap with credentialed connection string should succeed");

    assert_eq!(count, 2, "Should bootstrap 2 documents");

    // Verify events received
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    assert_eq!(events.len(), 2);

    let mut names: Vec<String> = events
        .iter()
        .filter_map(|e| {
            if let SourceChange::Insert { element } = &e.change {
                if let Element::Node { properties, .. } = element {
                    if let Some(ElementValue::String(s)) = properties.get("name") {
                        return Some(s.to_string());
                    }
                }
            }
            None
        })
        .collect();
    names.sort();

    assert_eq!(names, vec!["Alice", "Bob"]);
}
