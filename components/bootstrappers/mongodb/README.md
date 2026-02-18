# MongoDB Bootstrap Provider

A bootstrap provider for Drasi that reads initial data snapshots from MongoDB collections. This component enables continuous queries to start with a complete view of existing data before processing incremental changes.

## Overview

The MongoDB Bootstrap Provider streams existing documents from specified MongoDB collections to Drasi queries. It serves as the initial state mechanism for Continuous Queries, ensuring they have access to historical data.

### Key Capabilities

- **Collection Filtering**: Automatically maps Cypher query labels to MongoDB collections and validates their existence.
- **Per-Document Label Filtering**: Defensive per-document label check before creating Elements, saving allocations for any mismatched documents.
- **Efficient Batching**: Uses MongoDB cursors with configurable batch sizes (default: 1000) to stream large datasets without memory pressure.
- **Recursive BSON Handling**: Fully supports nested BSON Arrays and Documents, mapping them to Drasi `List` and `Object` types.
- **Binary Data Support**: Automatically converts BSON Binary fields to Base64-encoded strings.
- **Connection Reuse**: Efficiently manages MongoDB connections using connection pooling across multiple bootstrap requests.
- **Consistent ID Generation**: Deterministic Element IDs using the format `collection:{_id}`.

### Use Cases

- **Initial Query State**: Pre-load continuous queries with existing MongoDB documents.
- **Data Migration**: Stream a point-in-time view of a MongoDB database to downstream systems.
- **Testing**: Creating reproducible initial states for query verifications.

## Document-to-Graph Model Mapping

The bootstrap provider maps MongoDB's document model to Drasi's property graph model as follows:

| MongoDB Concept | Drasi Graph Concept | Example |
|-----------------|---------------------|---------|
| Collection name | **Node Label** | Collection `users` → Label `users` |
| Document | **Node** | One document = one node |
| `_id` field | **Element ID** | `_id: ObjectId("abc")` → ID `users:abc` |
| Top-level fields | **Node Properties** | `{ "name": "Alice", "age": 30 }` → properties map |
| Nested documents | **Object property** | `{ "address": { "city": "NYC" } }` → `Object` value |
| Arrays | **List property** | `{ "tags": ["a", "b"] }` → `List` value |

> [!NOTE]
> The `_id` field is **not** included in the property map — it is encoded into the Element ID as `{collection}:{_id}`.

### Mapping Example

Given this MongoDB document in the `products` collection:

```json
{
  "_id": ObjectId("507f1f77bcf86cd799439011"),
  "name": "Widget",
  "price": 29.99,
  "tags": ["sale", "popular"],
  "dimensions": { "width": 10, "height": 5 }
}
```

The bootstrap provider produces this Drasi graph node:

| Field | Value |
|-------|-------|
| **Element ID** | `products:507f1f77bcf86cd799439011` |
| **Label** | `products` |
| **Properties** | `name: "Widget"`, `price: 29.99`, `tags: ["sale", "popular"]`, `dimensions: { width: 10, height: 5 }` |

### Relationships

MongoDB does not have native relationships between documents. The bootstrap provider maps each document as an **independent node**. If your Cypher query references relationship labels (e.g., `MATCH (a)-[:KNOWS]->(b)`), those relationships must be provided by another source or bootstrap provider — the MongoDB bootstrap only emits nodes.

## MongoDB Prerequisites

### Server Version

- **Minimum**: MongoDB 4.0+
- **Recommended**: MongoDB 4.4+ for improved cursor performance

### Deployment Mode

- **Standalone**: Supported for bootstrap operations. Note that data may change during the scan since MongoDB standalone instances do not support snapshot reads.
- **Replica Set**: Recommended when used alongside the MongoDB Change Stream source, as change streams require a replica set (or sharded cluster). The bootstrap provider works with any deployment mode, but the companion CDC source requires `watch()`, which needs a replica set.

### User Permissions

The MongoDB user specified in the connection string must have:

```
read    — on the target database (to query collections)
```

Specifically, the following operations are used:

| Operation | Permission Required |
|-----------|-------------------|
| `listCollections` | `listCollections` privilege on the database |
| `find` | `find` privilege on each collection |

**Example — creating a minimal read-only user:**

```javascript
db.createUser({
  user: "drasi_bootstrap",
  pwd: "secure_password",
  roles: [{ role: "read", db: "your_database" }]
});
```

### Connection String Format

Standard MongoDB connection URIs are supported:

```
mongodb://user:password@host:port/database?options
mongodb+srv://user:password@cluster.example.com/database
```

## Configuration

The MongoDB Bootstrap Provider can be configured using either the builder pattern (preferred) or a configuration struct.

### Builder Pattern (Preferred)

The builder pattern provides a fluent API for constructing the provider:

```rust
use drasi_bootstrap_mongodb::MongoBootstrapProvider;

let provider = MongoBootstrapProvider::builder()
    .with_connection_string("mongodb://user:pass@localhost:27017/inventory")
    .with_collection("products")
    .with_collection("orders")
    .with_batch_size(2000)
    .build();
```

### Configuration Struct

Alternatively, use the `MongoBootstrapConfig` struct directly:

```rust
use drasi_bootstrap_mongodb::{MongoBootstrapProvider, MongoBootstrapConfig};

let config = MongoBootstrapConfig {
    connection_string: "mongodb://user:pass@localhost:27017".to_string(),
    database: "inventory".to_string(),
    collections: vec!["products".to_string(), "orders".to_string()],
    batch_size: 1000,
};

let provider = MongoBootstrapProvider::new(config);
```

## Configuration Options

| Name | Description | Data Type | Valid Values | Default |
|------|-------------|-----------|--------------|---------|
| `connection_string` | MongoDB connection URI | `String` | Valid MongoDB URI (e.g., `mongodb://localhost:27017`) | **Required** |
| `database` | Database to bootstrap from | `String` | Any valid MongoDB database name | **Required** |
| `collections` | Whitelist of collections to allow | `Vec<String>` | Valid collection names | `[]` (all requested labels allowed) |
| `batch_size` | Documents fetched per cursor batch | `u32` | Positive integer (recommended: 500–5000) | `1000` |

## Input Schema

### Collection Mapping

The provider maps Drasi Query **Node Labels** directly to MongoDB **Collection Names**. This mapping is case-sensitive.

- Cypher `MATCH (n:User)` → Scans `User` collection.
- Cypher `MATCH (n:products)` → Scans `products` collection.

**Validation:**
Before starting the scan, the provider checks if the requested collection actually exists in the database.
- If a collection does not exist, a `WARN` log is issued and that label is skipped.
- If no valid collections are found, the bootstrap process completes immediately with 0 records.

### Element ID Generation

Drasi requires a unique Element ID for every node. The MongoDB provider generates this deterministically:

Format: `{collection_name}:{_id_value}`

- **ObjectId**: `users:507f1f77bcf86cd799439011`
- **String _id**: `products:prod-123`
- **Integer _id**: `orders:1001`

*Note: The `_id` field itself is excluded from the property map since it is encoded in the Element ID.*

## Supported BSON Types

The provider maps BSON types to Drasi `ElementValue` as follows:

| BSON Type | Drasi ElementValue | Notes |
|-----------|--------------------|-------|
| **Double** | `Float` | 64-bit floating point |
| **String** | `String` | UTF-8 string |
| **Array** | `List` | 1:1 mapping (recursive) |
| **Document** | `Object` | 1:1 mapping (recursive) |
| **Boolean** | `Bool` | `true`/`false` |
| **Null** | `Null` | |
| **Undefined** | `Null` | Treated explicitly as Null |
| **Int32** | `Integer` | Promoted to 64-bit |
| **Int64** | `Integer` | 64-bit integer |
| **DateTime** | `String` | RFC3339 format (UTC) |
| **Timestamp** | `Integer` | Represents internal MongoDB timestamp (i64) |
| **ObjectId** | `String` | Hex string representation |
| **Binary** | `String` | **Base64 encoded string** |
| **Decimal128** | `String` | String representation to preserve precision |
| **RegularExpression** | `String` | Pattern string |
| **JavaScriptCode** | `String` | Source code string |
| **Symbol** | `String` | String value |
| **MinKey/MaxKey** | `String` | "MinKey" or "MaxKey" literal |

## Usage Examples

### Basic Bootstrap

```rust
use drasi_bootstrap_mongodb::MongoBootstrapProvider;
use drasi_lib::bootstrap::{BootstrapProvider, BootstrapRequest, BootstrapContext};
use drasi_lib::channels::bootstrap_channel;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

// Create provider
let provider = MongoBootstrapProvider::builder()
    .with_connection_string("mongodb://localhost:27017/inventory")
    .with_collection("products")
    .build();

// Create channel for receiving bootstrap events
let (tx, mut rx) = bootstrap_channel(1000);

// Create bootstrap request (from query)
let request = BootstrapRequest {
    query_id: "product-query".to_string(),
    node_labels: vec!["products".to_string()],
    relation_labels: vec![],
};

// Create context
let context = BootstrapContext {
    source_id: "mongo-source".to_string(),
    sequence_counter: Arc::new(AtomicU64::new(0)),
};

// Execute bootstrap
let count = provider.bootstrap(request, &context, tx, None).await?;
println!("Bootstrapped {} records", count);

// Receive events
while let Some(event) = rx.recv().await {
    println!("Received: {:?}", event);
}
```

### Usage with DrasiLib and MongoSource

This provider is designed to work seamlessly with the `MongoSource` component via the builder pattern:

```rust
use drasi_source_mongodb::{MongoSource, MongoSourceConfig};
use drasi_bootstrap_mongodb::{MongoBootstrapProvider, MongoBootstrapConfig};

let source = MongoSource::builder()
    .with_config(MongoSourceConfig {
        connection_string: "mongodb://localhost:27017/inventory".to_string(),
        ..Default::default()
    })
    .with_bootstrap_provider(
        MongoBootstrapProvider::builder()
            .with_connection_string("mongodb://localhost:27017/inventory")
            .with_collection("products")
            .build()
    )
    .build()?;
```

## Implementation Details

### Bootstrap Process

1.  **Connection**: Lazily establishes a MongoDB connection (reusing the client for subsequent requests).
2.  **Collection Resolution**: Resolves which collections to scan by intersecting configured collections with requested query labels.
3.  **Validation**: Queries `list_collection_names` to verify that resolved collections exist in the database.
4.  **Cursor Creation**: Opens a `find()` cursor for each valid collection with the configured batch size.
5.  **Per-Document Filtering**: Checks each document's label against requested labels before creating the Element (saves allocation).
6.  **BSON Conversion**: Converts each document's fields from BSON to Drasi `ElementValue` types.
7.  **Batching**: Accumulates events in a buffer (size configured by `batch_size`) before sending to the channel.
8.  **Error Handling**: Fails fast on any connection or cursor error to prevent partial/corrupt state.

### Consistency Guarantees

- **Sequential Collection Reads**: Collections are read sequentially; on standalone MongoDB instances, data may change between the start and end of the scan.
- **Replica Set Consistency**: When using a replica set, reads use the default read preference (primary), providing a consistent view within each collection.
- **Batch Ordering**: Events maintain sequential ordering within each collection.
- **Sequence Numbers**: Each event receives a unique, monotonically increasing sequence number (0-indexed, via `AtomicU64`).
- **Fail-Fast**: Any error during BSON conversion or network transmission immediately halts the bootstrap, allowing the system to retry cleanly.

### Performance Characteristics

- **Batch Size**: 1000 documents per cursor batch (configurable).
- **Memory Efficiency**: Streaming cursor approach avoids loading entire collections into memory.
- **Connection Management**: Lazy initialization with `OnceCell` — the MongoDB client is created once and reused across bootstrap requests.
- **Per-Document Filtering**: Label filtering happens before Element creation, avoiding unnecessary allocations.

### Error Handling

The provider returns errors for:

- **Connection failures**: Invalid connection string, network issues, authentication errors.
- **Missing collections**: Logged as warnings, skipped (not hard errors).
- **BSON conversion failures**: Unsupported `_id` types cause immediate error; field conversion is best-effort.
- **Channel send failures**: Indicates downstream consumer issues — fails fast with descriptive error.
- **Cursor errors**: Network interruptions or server issues during iteration — fails fast.

## Advanced Topics

### Custom ID Generation

Unlike relational databases where primary keys might be composite or missing, MongoDB enforces a unique `_id` field for every document. This provider always uses the `_id` field to generate the Drasi Element ID (`collection:_id`), ensuring consistency and uniqueness without additional configuration.

Supported `_id` types: `ObjectId`, `String`, `Int32`, `Int64`.

### Null Handling

- BSON `Null` values are mapped to `ElementValue::Null`.
- BSON `Undefined` values are also mapped to `ElementValue::Null`.
- The `_id` field is always expected to be present (MongoDB enforces this); an error is returned if it is missing.

### Logging

The provider uses the `log` crate with standard levels:

- **Info**: Connection status, collection counts, completion statistics.
- **Debug**: Detailed progress, per-document processing (at high verbosity).
- **Warn**: Missing collections, BSON conversion issues.
- **Error**: Connection failures, cursor errors, channel failures.

Enable logging in your application:

```rust
env_logger::init();
// Set RUST_LOG=debug for detailed logging
```

## Dependencies

- **drasi-core**: Graph data models (`Element`, `ElementReference`, `SourceChange`).
- **drasi-lib**: Core Drasi library for bootstrap provider trait and channels.
- **mongodb**: MongoDB Rust driver (v2.8.2) with `tokio-runtime` feature.
- **bson**: BSON serialization/deserialization (v2.15.0) with `chrono-0_4` feature.
- **tokio**: Async runtime for cursor iteration and channel operations.
- **futures**: Stream utilities (`TryStreamExt` for cursor iteration).
- **anyhow**: Error handling and context.
- **log**: Logging framework.
- **chrono**: Timestamp generation for bootstrap events.
- **base64**: Encoding BSON Binary values.

## Testing

The provider includes unit tests and integration tests:

```bash
# Run unit tests (no external dependencies)
cargo test -p drasi-bootstrap-mongodb --lib

# Run integration tests (requires Docker)
cargo test -p drasi-bootstrap-mongodb --test mongo_integration -- --ignored

# Run with debug logging
RUST_LOG=debug cargo test -p drasi-bootstrap-mongodb -- --nocapture
```

### Test Coverage

**Unit Tests (32 tests):**
- BSON-to-ElementValue conversion for all supported types (24 tests)
- Collection resolution logic: empty request, open config, strict config, partial overlap, disjoint (5 tests)
- Sequence number monotonicity through `process_document` (1 test)
- `process_document` correctness: element IDs, labels, properties, `_id` exclusion (1 test)
- `send_batch` event delivery via mpsc channel (1 test)

**Integration Tests (5 tests, require Docker):**
- Full bootstrap with real MongoDB replica set
- Empty collection bootstrap
- Label filtering across multiple collections
- BSON type conversion end-to-end through real MongoDB
- Nonexistent collection graceful handling

## Troubleshooting

### "Collection ... does not exist, skipping"

**Symptom**: Warn log appears, and no data is bootstrapped for a specific label.
**Cause**: The requested node label in your Query (e.g., `MATCH (n:Users)`) does not match the case-sensitive name of the MongoDB collection (e.g., `users`).
**Solution**: Ensure your Cypher query labels exactly match your MongoDB collection names.

### Connection Failures

**Symptom**: `AuthFailed` or `Timeout` errors.
**Solution**:
- Check that the `connection_string` includes correct username/password.
- Ensure the MongoDB host is reachable from the Drasi container.
- If using Atlas, ensure the IP whitelist includes the Drasi service.

### Memory Issues with Large Collections

**Symptom**: High memory usage during bootstrap.
**Solution**:
- Batch processing (default 1000 documents) already limits memory.
- Reduce `batch_size` for very large documents.
- Monitor downstream consumer to ensure it's processing events promptly.

## License

Copyright 2025 The Drasi Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
