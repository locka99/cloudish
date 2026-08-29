
//! DynamoDB types: AttributeValue, TableMeta, KeySchema, etc.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// DynamoDB attribute value (externally tagged).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum AttributeValue {
    S(String),
    N(String),
    B(String),
    #[serde(rename = "BOOL")]
    Bool(bool),
    #[serde(rename = "NULL")]
    Null(bool),
    L(Vec<AttributeValue>),
    M(HashMap<String, AttributeValue>),
    SS(Vec<String>),
    NS(Vec<String>),
    BS(Vec<String>),
}

impl AttributeValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            AttributeValue::S(_) => "S",
            AttributeValue::N(_) => "N",
            AttributeValue::B(_) => "B",
            AttributeValue::Bool(_) => "BOOL",
            AttributeValue::Null(_) => "NULL",
            AttributeValue::L(_) => "L",
            AttributeValue::M(_) => "M",
            AttributeValue::SS(_) => "SS",
            AttributeValue::NS(_) => "NS",
            AttributeValue::BS(_) => "BS",
        }
    }

    pub fn as_n_f64(&self) -> Option<f64> {
        match self {
            AttributeValue::N(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_s(&self) -> Option<&str> {
        match self {
            AttributeValue::S(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<Vec<u8>> {
        match self {
            AttributeValue::B(b) => {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.decode(b).ok()
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySchemaElement {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
    #[serde(rename = "KeyType")]
    pub key_type: String, // "HASH" or "RANGE"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeDefinition {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
    #[serde(rename = "AttributeType")]
    pub attribute_type: String, // "S", "N", "B"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub table_name: String,
    pub status: String,
    pub creation_datetime: f64,
    pub key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub billing_mode: String,
    pub read_capacity_units: u64,
    pub write_capacity_units: u64,
    pub table_arn: String,
    pub table_id: String,
    pub item_count: i64,
    pub table_size_bytes: i64,
    pub stream_enabled: bool,
    pub stream_view_type: Option<String>,
    pub stream_arn: Option<String>,
    pub stream_label: Option<String>,
    pub ttl_attribute: Option<String>,
    pub ttl_enabled: bool,
}

impl TableMeta {
    pub fn hash_key(&self) -> Option<&str> {
        self.key_schema
            .iter()
            .find(|k| k.key_type == "HASH")
            .map(|k| k.attribute_name.as_str())
    }

    pub fn range_key(&self) -> Option<&str> {
        self.key_schema
            .iter()
            .find(|k| k.key_type == "RANGE")
            .map(|k| k.attribute_name.as_str())
    }

    pub fn to_describe_json(&self) -> serde_json::Value {
        use serde_json::json;
        let key_schema: Vec<serde_json::Value> = self
            .key_schema
            .iter()
            .map(|k| {
                json!({
                    "AttributeName": k.attribute_name,
                    "KeyType": k.key_type,
                })
            })
            .collect();

        let attr_defs: Vec<serde_json::Value> = self
            .attribute_definitions
            .iter()
            .map(|a| {
                json!({
                    "AttributeName": a.attribute_name,
                    "AttributeType": a.attribute_type,
                })
            })
            .collect();

        let mut table = json!({
            "TableName": self.table_name,
            "TableStatus": self.status,
            "CreationDateTime": self.creation_datetime,
            "KeySchema": key_schema,
            "AttributeDefinitions": attr_defs,
            "BillingModeSummary": {"BillingMode": self.billing_mode},
            "ProvisionedThroughput": {
                "ReadCapacityUnits": self.read_capacity_units,
                "WriteCapacityUnits": self.write_capacity_units,
                "NumberOfDecreasesToday": 0,
            },
            "TableArn": self.table_arn,
            "TableId": self.table_id,
            "ItemCount": self.item_count,
            "TableSizeBytes": self.table_size_bytes,
        });

        if self.stream_enabled {
            let view_type = self.stream_view_type.clone().unwrap_or_default();
            table["StreamSpecification"] = json!({
                "StreamEnabled": true,
                "StreamViewType": view_type,
            });
            if let Some(arn) = &self.stream_arn {
                table["LatestStreamArn"] = json!(arn);
            }
            if let Some(label) = &self.stream_label {
                table["LatestStreamLabel"] = json!(label);
            }
        }

        table
    }
}

pub type Item = HashMap<String, AttributeValue>;
