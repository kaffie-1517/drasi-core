# MongoDB Bootstrap Provider

A bootstrap provider for Drasi that reads initial data snapshots from MongoDB collections. This component enables continuous queries to start with a complete view of existing data before processing incremental changes.

## Overview

The MongoDB Bootstrap Provider streams existing documents from specified MongoDB collections to Drasi queries. It serves as the initial state mechanism for Continuous Queries, ensuring they have access to historical data.

### Key Capabilities

- **Collection Filtering**: Automatically mxaps Cypher query labels to MongoDB collections and validates their existence.
- **Efficient Batching**: Uses MongoDB cursors with configurable batch sizes (default: 1000) to stream large datasets without memory pressure.
- **Recursive BSON Handling**: Fully supports nested BSON Arrays and Documents, mapping them to Drasi `List` and `Object` types.
- **Binary Data Support**: Automatically converts BSON Binary fields to Base64-encoded strings.
- **Connection Reuse**: Efficiently manages MongoDB connections using connection pooling across multiple bootstrap requests.
- **Consistent ID Generation**: deterministic Element IDs using the format `collection:{_id}`.

### Use Cases

- **Initial Query State**: Pre-load continuous queries with existing MongoDB documents.
- **Data Migration**: Stream a point-in-time view of a MongoDB database to downstream systems.
- **Testing**: creating reproducible initial states for query verifications.

## Configuration

The MongoDB Bootstrap Provider can be configured using either the builder pattern (preferred) or a configuration struct.

### Builder Pattern (Preferred)

The builder pattern provides a fluent API for constructing the provider:

```rust
use drasi_bootstrap_mongodb::MongoBootstrapProvider;

let provider = MongoBootstrapProvider::builder()
    .with_connection_string("mongodb://user:pass@localhost:27017")
    .with_database("inventory")
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

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `connection_string` | `String` | Yes | - | MongoDB connection URI (e.g., `mongodb://localhost:27017`). |
| `database` | `String` | Yes | - | The name of the database to bootstrap from. |
| `collections` | `List<String>` | No | `[]` (All) | Whitelist of collections to allow. If empty, all requested query labels are allowed (if they exist). |
| `batch_size` | `Integer` | No | `1000` | Number of documents to fetch per batch from the cursor. |

## Input Schema

### Collection Mapping

The provider maps Drasi Query **Node Labels** directly to MongoDB **Collection Names**. This mapping is case-sensitive.

- Cypher `MATCH (n:User)` -> Scans `User` collection.
- Cypher `MATCH (n:products)` -> Scans `products` collection.

**Validation:**
Before starting the scan, the provider checks if the requested collection actually exists in the database.
- If a collection does not exist, a `WARN` log is issued and that label is skipped.
- If no valid collections are found, the bootstrap process completes immediately with 0 records.

### Element ID Generation

Drasi requires a unique Element ID for every node. The MongoDB provider generates this implementation-deterministically:

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
| **Undefined** | `Null` | Treating explicitly as Null |
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

### Usage with DrasiLib and MongoSource

This provider is designed to work seamlessly with the `MongoSource` component via the builder pattern:

```rust
use drasi_source_mongodb::{MongoSource, MongoSourceConfig};
use drasi_bootstrap_mongodb::{MongoBootstrapProvider, MongoBootstrapConfig};

let source = MongoSource::builder()
    .with_config(MongoSourceConfig {
        connection_string: "mongodb://localhost:27017".to_string(),
        database: "inventory".to_string(),
        ..Default::default()
    })
    .with_bootstrap_provider(
        MongoBootstrapProvider::builder()
            .with_connection_string("mongodb://localhost:27017")
            .with_database("inventory")
            .with_collection("products")
            .build()
    )
    .build()?;
```

## Implementation Details

### Bootstrap Process

1.  **Connection**: Lazily establishes a MongoDB connection (reusing the client for subsequent requests).
2.  **Validation**: Queries `list_collection_names` to verify that requested node labels correspond to actual collections.
3.  **Cursor Creation**: Opens a `find()` cursor for each valid collection.
4.  **Streaming**: Iterates over documents, converting BSON to `Element` types.
5.  **Batching**: Accumulates events in a buffer (size configured by `batch_size`) before sending to the channel.
6.  **Error Handling**: Fails fast on any connection or cursor error to prevent partial/corrupt state.

### Consistency Notes

-   **Isolation**: The provider currently reads collections sequentially. If using a standalone instance, data may change between the start and end of the scan.
-   **Fail-Fast**: Any error during BSON conversion or network transmission immediately halts the bootstrap, allowing the system to retry cleanly.

## Advanced Topics

### Custom ID Generation

Unlike relational databases where primary keys might be composite or missing, MongoDB enforces a unique `_id` field for every document. This provider always uses the `_id` field to generate the Drasi Element ID (`collection:_id`), ensuring consistency and uniqueness without additional configuration.

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

## Troubleshooting

### "Collection ... does not exist, skipping"

**Symptom**: Warn log appears, and no data is bootstrapped for a specific label.
**Cause**: The requested node label in your Query (e.g., `MATCH (n:Users)`) does not match the case-sensitive name of the MongoDB collection (e.g., `users`).
**Solution**: Ensure your Cypher query labels exact match your MongoDB collection names.

### Connection Failures

**Symptom**: `AuthFailed` or `Timeout` errors.
**Solution**:
- Check that the `connection_string` includes correct username/password.
- Ensure the MongoDB host is reachable from the Drasi container.
- If using Atlas, ensure the IP whitelist includes the Drasi service.

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
