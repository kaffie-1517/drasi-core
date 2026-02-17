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
            // Convert bson::DateTime → chrono::DateTime<Utc> → RFC 3339 string.
            // Using chrono::to_rfc3339() is equivalent to the bson
            // try_to_rfc3339_string() path but infallible after the conversion.
            let dt: DateTime<Utc> = (*dt).into();
            ElementValue::String(Arc::from(dt.to_rfc3339()))
        }
        Bson::ObjectId(oid) => ElementValue::String(Arc::from(oid.to_hex())),
        Bson::Binary(bin) => {
            // Contract: Binary → String (Base64 encoded)
            use base64::{engine::general_purpose, Engine as _};
            ElementValue::String(Arc::from(general_purpose::STANDARD.encode(&bin.bytes)))
        }
        Bson::Decimal128(d) => {
            // Use string representation to preserve full precision.
            ElementValue::String(Arc::from(d.to_string()))
        }
        Bson::RegularExpression(regex) => {
            ElementValue::String(Arc::from(regex.pattern.as_str()))
        }
        Bson::JavaScriptCode(code) => ElementValue::String(Arc::from(code.as_str())),
        Bson::JavaScriptCodeWithScope(code_with_scope) => {
            ElementValue::String(Arc::from(code_with_scope.code.as_str()))
        }
        Bson::Timestamp(ts) => {
            // MongoDB internal Timestamp (not wall-clock DateTime). Encode as
            // a single i64 so callers can unpack time and increment if needed.
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

pub fn extract_element_id(doc: &Document, collection: &str) -> Result<String> {
    let id_val = doc
        .get("_id")
        .ok_or_else(|| anyhow!("Document missing required '_id' field"))?;

    let id_str = match id_val {
        Bson::ObjectId(oid) => oid.to_hex(),
        Bson::String(s) => s.clone(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        other => other.to_string(),
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
        let doc = doc! { "_id": "user-alice-123" };
        let id = extract_element_id(&doc, "users").unwrap();
        assert_eq!(id, "users:user-alice-123");
    }

    #[test]
    fn test_extract_element_id_int32() {
        let doc = doc! { "_id": 12345_i32 };
        let id = extract_element_id(&doc, "orders").unwrap();
        assert_eq!(id, "orders:12345");
    }

    #[test]
    fn test_extract_element_id_int64() {
        let doc = doc! { "_id": 9_876_543_210_i64 };
        let id = extract_element_id(&doc, "events").unwrap();
        assert_eq!(id, "events:9876543210");
    }

    #[test]
    fn test_extract_element_id_missing_field() {
        let doc = doc! { "name": "Alice" };
        let result = extract_element_id(&doc, "users");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("missing required '_id'"));
    }

    #[test]
    fn test_double_to_float() {
        let val = bson_to_element_value(&Bson::Double(3.14));
        assert_eq!(val, ElementValue::Float(OrderedFloat(3.14)));
    }

    #[test]
    fn test_string_to_string() {
        let val = bson_to_element_value(&Bson::String("hello".to_string()));
        assert_eq!(val, ElementValue::String(Arc::from("hello")));
    }

    #[test]
    fn test_boolean_true() {
        let val = bson_to_element_value(&Bson::Boolean(true));
        assert_eq!(val, ElementValue::Bool(true));
    }

    #[test]
    fn test_boolean_false() {
        let val = bson_to_element_value(&Bson::Boolean(false));
        assert_eq!(val, ElementValue::Bool(false));
    }

    #[test]
    fn test_null_to_null() {
        let val = bson_to_element_value(&Bson::Null);
        assert_eq!(val, ElementValue::Null);
    }

    #[test]
    fn test_undefined_to_null() {
        let val = bson_to_element_value(&Bson::Undefined);
        assert_eq!(val, ElementValue::Null);
    }

    #[test]
    fn test_int32_to_integer() {
        let val = bson_to_element_value(&Bson::Int32(42));
        assert_eq!(val, ElementValue::Integer(42));
    }

    #[test]
    fn test_int64_to_integer() {
        let val = bson_to_element_value(&Bson::Int64(9_876_543_210));
        assert_eq!(val, ElementValue::Integer(9_876_543_210));
    }

    #[test]
    fn test_int32_promotion_to_i64() {
        // Int32 must be promoted to i64 (ElementValue::Integer is i64)
        let val = bson_to_element_value(&Bson::Int32(i32::MAX));
        assert_eq!(val, ElementValue::Integer(i32::MAX as i64));
    }

    #[test]
    fn test_objectid_to_hex_string() {
        let oid = ObjectId::from_str("507f1f77bcf86cd799439011").unwrap();
        let val = bson_to_element_value(&Bson::ObjectId(oid));
        assert_eq!(val, ElementValue::String(Arc::from("507f1f77bcf86cd799439011")));
    }

    #[test]
    fn test_datetime_to_rfc3339_string() {
        use bson::DateTime as BsonDateTime;
        // 2024-01-15T12:00:00Z in milliseconds since epoch
        let ms: i64 = 1_705_320_000_000;
        let bson_dt = BsonDateTime::from_millis(ms);
        let val = bson_to_element_value(&Bson::DateTime(bson_dt));
        // Must be a String in RFC 3339 / ISO 8601 UTC format
        if let ElementValue::String(s) = val {
            assert!(s.contains("2024-01-15"), "expected date 2024-01-15 in '{s}'");
            assert!(s.ends_with('Z') || s.contains('+'), "expected UTC marker in '{s}'");
        } else {
            panic!("expected ElementValue::String for DateTime, got {val:?}");
        }
    }

    #[test]
    fn test_binary_to_base64_string() {
        use base64::{engine::general_purpose, Engine as _};
        let bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let bin = bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: bytes.clone(),
        };
        let val = bson_to_element_value(&Bson::Binary(bin));
        let expected = general_purpose::STANDARD.encode(&bytes);
        assert_eq!(val, ElementValue::String(Arc::from(expected.as_str())));
    }

    #[test]
    fn test_decimal128_to_string() {
        // Decimal128::from_str requires the bson feature "decimal128" or parse via string
        let d = bson::Decimal128::from_bytes([0u8; 16]);
        let val = bson_to_element_value(&Bson::Decimal128(d));
        assert!(matches!(val, ElementValue::String(_)));
    }

    #[test]
    fn test_array_to_list() {
        let bson_arr = Bson::Array(vec![
            Bson::String("a".to_string()),
            Bson::String("b".to_string()),
            Bson::Int32(1),
        ]);
        let val = bson_to_element_value(&bson_arr);
        if let ElementValue::List(items) = val {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], ElementValue::String(Arc::from("a")));
            assert_eq!(items[1], ElementValue::String(Arc::from("b")));
            assert_eq!(items[2], ElementValue::Integer(1));
        } else {
            panic!("expected List, got {val:?}");
        }
    }

    #[test]
    fn test_recursive_array_conversion() {
        let doc = doc! { "list": ["a", "b", 1_i32] };
        let val = bson_to_element_value(&Bson::Document(doc));
        if let ElementValue::Object(map) = val {
            let list = map.get("list").unwrap();
            if let ElementValue::List(items) = list {
                assert_eq!(items.len(), 3);
                assert!(matches!(items[0], ElementValue::String(_)));
                assert!(matches!(items[2], ElementValue::Integer(1)));
            } else {
                panic!("expected List");
            }
        } else {
            panic!("expected Object");
        }
    }

    #[test]
    fn test_recursive_document_conversion() {
        let doc = doc! {
            "nested": {
                "a": 1_i32,
                "b": true
            }
        };
        let val = bson_to_element_value(&Bson::Document(doc));
        if let ElementValue::Object(map) = val {
            let nested = map.get("nested").unwrap();
            if let ElementValue::Object(inner) = nested {
                assert_eq!(inner.get("a"), Some(&ElementValue::Integer(1)));
                assert_eq!(inner.get("b"), Some(&ElementValue::Bool(true)));
            } else {
                panic!("expected nested Object");
            }
        } else {
            panic!("expected Object");
        }
    }

    #[test]
    fn test_empty_array_to_empty_list() {
        let val = bson_to_element_value(&Bson::Array(vec![]));
        assert_eq!(val, ElementValue::List(vec![]));
    }

    #[test]
    fn test_empty_document_to_empty_object() {
        use bson::Document;
        let val = bson_to_element_value(&Bson::Document(Document::new()));
        assert!(matches!(val, ElementValue::Object(_)));
    }
}