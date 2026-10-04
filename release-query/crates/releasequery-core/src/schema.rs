use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

pub const KEYS: [&str; 2] = ["tenant_id", "event_id"];
pub const COMPARE: [&str; 5] = [
    "event_time",
    "units",
    "plan_id",
    "billable_units",
    "rule_version",
];

pub fn live_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("units", DataType::Decimal128(18, 6), true),
        Field::new("plan_id", DataType::Utf8, true),
        Field::new("billable_units", DataType::Decimal128(18, 6), true),
        Field::new("rule_version", DataType::Utf8, true),
    ]))
}

pub fn detail_schema() -> SchemaRef {
    let mut fields: Vec<Field> = live_schema()
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    fields.push(Field::new("is_deleted", DataType::Boolean, false));
    Arc::new(Schema::new(fields))
}

pub fn diff_schema() -> SchemaRef {
    // Null before/after structs have nullable physical children, matching the SQL
    // artifact schema. Present-record keys are validated independently.
    let children: Fields = live_schema()
        .fields()
        .iter()
        .map(|f| Arc::new(Field::new(f.name(), f.data_type().clone(), true)))
        .collect();
    Arc::new(Schema::new(vec![
        Field::new("change_type", DataType::Utf8, false),
        Field::new(
            "key",
            DataType::Struct(children.iter().take(2).cloned().collect()),
            false,
        ),
        Field::new("before", DataType::Struct(children.clone()), true),
        Field::new("after", DataType::Struct(children), true),
    ]))
}
