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

//! BSON to Element conversion utilities

use anyhow::{anyhow, Result};
use bson::{Bson, Document};
use chrono::{DateTime, Utc};
use drasi_core::models::{ElementPropertyMap, ElementValue};
use ordered_float::OrderedFloat;
use std::sync::Arc;

/// Convert a BSON type to an ElementValue
pub fn bson_to_element_value(bson: &Bson) -> ElementValue {
    match bson {
        Bson::Double(f) => ElementValue::Float(OrderedFloat(*f)),
        Bson::String(s) => ElementValue::String(Arc::from(s.as_str())),
        Bson::Array(arr) => {
            let values: Vec<ElementValue> = arr.iter().map(bson_to_element_value).collect();
            ElementValue::List(values)
        }
        Bson::Document(doc) => {
            let mut properties = ElementPropertyMap::new();
            for (key, value) in doc {
                properties.insert(key, bson_to_element_value(value));
            }
            ElementValue::Object(properties)
        }
        Bson::Boolean(b) => ElementValue::Bool(*b),
        Bson::Null => ElementValue::Null,
        Bson::Int32(i) => ElementValue::Integer(*i as i64),
        Bson::Int64(i) => ElementValue::Integer(*i),
        Bson::DateTime(dt) => {
            // Convert bson::DateTime to chrono::DateTime<Utc>
            let dt: DateTime<Utc> = (*dt).into();
            // Format as RFC3339 string
            ElementValue::String(Arc::from(dt.to_rfc3339()))
        }
        Bson::ObjectId(oid) => ElementValue::String(Arc::from(oid.to_hex())),
        Bson::Binary(bin) => {
            // Requirement matches "Binary -> String (Base64 encoded)"
            use base64::{engine::general_purpose, Engine as _};
            ElementValue::String(Arc::from(general_purpose::STANDARD.encode(&bin.bytes)))
        }
        Bson::Decimal128(d) => ElementValue::String(Arc::from(d.to_string())),
        Bson::RegularExpression(regex) => ElementValue::String(Arc::from(regex.pattern.as_str())),
        Bson::JavaScriptCode(code) => ElementValue::String(Arc::from(code.as_str())),
        Bson::JavaScriptCodeWithScope(code_with_scope) => {
            ElementValue::String(Arc::from(code_with_scope.code.as_str()))
        }
        Bson::Timestamp(ts) => {
            // MongoDB Timestamp is internal type, distinct from Date.
            // Usually (time, increment). Let's represent as Integer (time << 32 | increment) or String.
            // Given requirements mapped "Undefined -> Null", "Binary -> String".
            // Let's map Timestamp to Integer (time part). Or String for safety.
            // Let's use Integer for now as it fits i64 mostly.
            let val = ((ts.time as i64) << 32) | (ts.increment as i64);
            ElementValue::Integer(val)
        }
        Bson::Symbol(s) => ElementValue::String(Arc::from(s.as_str())),
        Bson::Undefined => ElementValue::Null,
        Bson::MinKey => ElementValue::String(Arc::from("MinKey")),
        Bson::MaxKey => ElementValue::String(Arc::from("MaxKey")),
        _ => ElementValue::String(Arc::from(format!("{bson:?}"))),
    }
}

/// Extract Element ID from BSON document
/// Format: {collection}:{_id_serialized}
pub fn extract_element_id(doc: &Document, collection: &str) -> Result<String> {
    let id_val = doc
        .get("_id")
        .ok_or_else(|| anyhow!("Document missing _id field"))?;

    let id_str = match id_val {
        Bson::ObjectId(oid) => oid.to_hex(),
        Bson::String(s) => s.clone(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        _ => id_val.to_string(), // Fallback for complex IDs
    };

    Ok(format!("{collection}:{id_str}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use bson::oid::ObjectId;
    use std::str::FromStr;

    #[test]
    fn test_extract_element_id_objectid() {
        let oid = ObjectId::from_str("507f1f77bcf86cd799439011").unwrap();
        let doc = doc! { "_id": oid };
        let id = extract_element_id(&doc, "users").unwrap();
        assert_eq!(id, "users:507f1f77bcf86cd799439011");
    }

    #[test]
    fn test_extract_element_id_string() {
        let doc = doc! { "_id": "user-123" };
        let id = extract_element_id(&doc, "users").unwrap();
        assert_eq!(id, "users:user-123");
    }

    #[test]
    fn test_recursive_array_conversion() {
        let doc = doc! {
            "list": ["a", "b", 1]
        };
        let val = bson_to_element_value(&bson::Bson::Document(doc));

        if let ElementValue::Object(map) = val {
            let list = map.get("list").unwrap();
            match list {
                ElementValue::List(items) => {
                    assert_eq!(items.len(), 3);
                    assert!(matches!(items[0], ElementValue::String(_)));
                    assert!(matches!(items[2], ElementValue::Integer(1)));
                }
                _ => panic!("Expected List"),
            }
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_recursive_document_conversion() {
        let doc = doc! {
            "nested": {
                "a": 1,
                "b": true
            }
        };
        let val = bson_to_element_value(&bson::Bson::Document(doc));

        if let ElementValue::Object(map) = val {
            let nested = map.get("nested").unwrap();
            if let ElementValue::Object(inner) = nested {
                assert_eq!(inner.get("a"), Some(&ElementValue::Integer(1)));
                assert_eq!(inner.get("b"), Some(&ElementValue::Bool(true)));
            } else {
                panic!("Expected nested Object");
            }
        } else {
            panic!("Expected Object");
        }
    }
}
