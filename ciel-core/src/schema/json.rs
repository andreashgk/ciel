use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use crate::schema::ArrayRules;
use crate::schema::Metadata;
use crate::schema::NumberRules;
use crate::schema::ObjectRules;
use crate::schema::Schema;
use crate::schema::SchemaKind;
use crate::schema::StringRules;

impl Schema {
    /// Converts the schema into a valid JSON Schema `serde_json::Value`.
    pub fn to_json(&self) -> Value {
        let mut map = Map::new();

        self.metadata.add_json_fields(&mut map);
        self.kind.add_json_fields(&mut map);

        Value::Object(map)
    }
}

impl Metadata {
    fn add_json_fields(&self, map: &mut Map<String, Value>) {
        if let Some(ref title) = self.title {
            map.insert("title".to_string(), Value::String(title.clone()));
        }
        if let Some(ref description) = self.description {
            map.insert(
                "description".to_string(),
                Value::String(description.clone()),
            );
        }
    }
}

impl SchemaKind {
    fn add_json_fields(&self, map: &mut Map<String, Value>) {
        match self {
            SchemaKind::String(rules) => {
                map.insert("type".to_string(), Value::String("string".to_string()));
                rules.add_json_fields(map);
            }
            SchemaKind::Number(rules) => {
                map.insert("type".to_string(), Value::String("number".to_string()));
                rules.add_json_fields(map);
            }
            SchemaKind::Integer(rules) => {
                map.insert("type".to_string(), Value::String("integer".to_string()));
                rules.add_json_fields(map);
            }
            SchemaKind::Boolean => {
                map.insert("type".to_string(), Value::String("boolean".to_string()));
            }
            SchemaKind::Null => {
                map.insert("type".to_string(), Value::String("null".to_string()));
            }
            SchemaKind::Object(rules) => {
                map.insert("type".to_string(), Value::String("object".to_string()));
                rules.add_json_fields(map);
            }
            SchemaKind::Array(rules) => {
                map.insert("type".to_string(), Value::String("array".to_string()));
                rules.add_json_fields(map);
            }
            SchemaKind::Union(variants) => {
                for v in variants {
                    let mut variant_map = Map::new();
                    v.add_json_fields(&mut variant_map);
                    merge_json_maps(map, variant_map);
                }
            }
            SchemaKind::AnyOf(schemas) => {
                let json_schemas: Vec<Value> = schemas.iter().map(|s| s.to_json()).collect();
                map.insert("anyOf".to_string(), Value::Array(json_schemas));
            }
            SchemaKind::AllOf(schemas) => {
                let json_schemas: Vec<Value> = schemas.iter().map(|s| s.to_json()).collect();
                map.insert("allOf".to_string(), Value::Array(json_schemas));
            }
            SchemaKind::OneOf(schemas) => {
                let json_schemas: Vec<Value> = schemas.iter().map(|s| s.to_json()).collect();
                map.insert("oneOf".to_string(), Value::Array(json_schemas));
            }
            SchemaKind::Not(schema) => {
                map.insert("not".to_string(), schema.to_json());
            }
            SchemaKind::Any => {}
        }
    }
}

impl StringRules {
    fn add_json_fields(&self, map: &mut Map<String, Value>) {
        if let Some(min) = self.min_length {
            map.insert("minLength".to_string(), json!(min));
        }
        if let Some(max) = self.max_length {
            map.insert("maxLength".to_string(), json!(max));
        }
        if let Some(ref pattern) = self.pattern {
            map.insert("pattern".to_string(), Value::String(pattern.clone()));
        }
        if let Some(ref format_str) = self.format {
            map.insert("format".to_string(), Value::String(format_str.clone()));
        }
    }
}

impl NumberRules {
    fn add_json_fields(&self, map: &mut Map<String, Value>) {
        if let Some(mo) = self.multiple_of {
            map.insert("multipleOf".to_string(), json!(mo));
        }
        if let Some(min) = self.minimum {
            map.insert("minimum".to_string(), json!(min));
        }
        if let Some(emin) = self.exclusive_minimum {
            map.insert("exclusiveMinimum".to_string(), json!(emin));
        }
        if let Some(max) = self.maximum {
            map.insert("maximum".to_string(), json!(max));
        }
        if let Some(emax) = self.exclusive_maximum {
            map.insert("exclusiveMaximum".to_string(), json!(emax));
        }
    }
}

impl ArrayRules {
    fn add_json_fields(&self, map: &mut Map<String, Value>) {
        if let Some(ref items) = self.items {
            map.insert("items".to_string(), items.to_json());
        }
        if let Some(min) = self.min_items {
            map.insert("minItems".to_string(), json!(min));
        }
        if let Some(max) = self.max_items {
            map.insert("maxItems".to_string(), json!(max));
        }
    }
}

impl ObjectRules {
    fn add_json_fields(&self, map: &mut Map<String, Value>) {
        if !self.properties.is_empty() {
            let mut props = Map::new();
            for (k, v) in &self.properties {
                props.insert(k.clone(), v.to_json());
            }
            map.insert("properties".to_string(), Value::Object(props));
        }

        if let Some(ap) = self.additional_properties {
            map.insert("additionalProperties".to_string(), json!(ap));
        }

        if !self.required.is_empty() {
            let reqs: Vec<Value> = self
                .required
                .iter()
                .map(|r| Value::String(r.clone()))
                .collect();
            map.insert("required".to_string(), Value::Array(reqs));
        }

        if let Some(min) = self.min_properties {
            map.insert("minProperties".to_string(), json!(min));
        }
        if let Some(max) = self.max_properties {
            map.insert("maxProperties".to_string(), json!(max));
        }
    }
}

fn merge_json_maps(target: &mut Map<String, Value>, source: Map<String, Value>) {
    for (k, v) in source {
        if let Some(existing) = target.get_mut(&k) {
            match k.as_str() {
                "type" => {
                    let mut types = match existing.take() {
                        Value::Array(a) => a,
                        Value::String(s) => vec![Value::String(s)],
                        other => vec![other],
                    };

                    let incoming_types = match v {
                        Value::Array(a) => a,
                        Value::String(s) => vec![Value::String(s)],
                        other => vec![other],
                    };

                    // Merge uniquely
                    for t in incoming_types {
                        if !types.contains(&t) {
                            types.push(t);
                        }
                    }

                    if types.len() == 1 {
                        *existing = types.pop().unwrap();
                    } else {
                        *existing = Value::Array(types);
                    }
                }
                // For minimums, we want the largest value
                "minLength" | "minItems" | "minProperties" | "minimum" | "exclusiveMinimum" => {
                    if let (Some(e_val), Some(v_val)) = (existing.as_f64(), v.as_f64()) {
                        if v_val > e_val {
                            *existing = v;
                        }
                    }
                }
                // For maximums, we want the smallest value
                "maxLength" | "maxItems" | "maxProperties" | "maximum" | "exclusiveMaximum" => {
                    if let (Some(e_val), Some(v_val)) = (existing.as_f64(), v.as_f64()) {
                        if v_val < e_val {
                            *existing = v;
                        }
                    }
                }
                _ => {
                    if existing.is_object() && v.is_object() {
                        // Recursively merge nested objects (like `properties`)
                        if let (Value::Object(target_obj), Value::Object(source_obj)) =
                            (existing, v)
                        {
                            merge_json_maps(target_obj, source_obj);
                        }
                    } else if existing.is_array() && v.is_array() {
                        // Merge arrays (like `required`) without duplicating items
                        if let (Value::Array(target_arr), Value::Array(source_arr)) = (existing, v)
                        {
                            for item in source_arr {
                                if !target_arr.contains(&item) {
                                    target_arr.push(item);
                                }
                            }
                        }
                    } else {
                        // For non-conflict scalar fields or completely different types, overwrite
                        *existing = v;
                    }
                }
            }
        } else {
            target.insert(k, v);
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::schema::NumberRules;
    use crate::schema::ObjectRules;
    use crate::schema::Schema;
    use crate::schema::SchemaKind;
    use crate::schema::StringRules;
    use serde_json::json;

    #[test]
    fn empty() {
        let schema = Schema::any().to_json();
        assert_eq!(schema, json!({}));
    }

    #[test]
    fn primitive_union() {
        let schema = Schema::union([
            SchemaKind::String(StringRules::new().min_length(1)),
            SchemaKind::Null,
        ])
        .description("test")
        .to_json();

        assert_eq!(
            schema,
            json!({
                "description": "test",
                "type": ["string", "null"],
                "minLength": 1,
            }),
        );
    }

    #[test]
    fn same_type_union() {
        let schema = Schema::union([
            SchemaKind::String(StringRules::new().min_length(2)),
            SchemaKind::String(StringRules::new().min_length(1)),
        ])
        .description("test")
        .to_json();

        assert_eq!(
            schema,
            json!({
                "description": "test",
                "type": "string",
                "minLength": 2,
            }),
        );
    }

    #[test]
    fn any_of() {
        let schema = Schema::any_of([
            Schema::object(ObjectRules::new().property("f1", Schema::boolean()))
                .description("first type"),
            Schema::number(NumberRules::new()).description("second type"),
        ])
        .description("test")
        .to_json();

        assert_eq!(
            schema,
            json!({
                "description": "test",
                "anyOf": [
                    {
                        "description": "first type",
                        "type": "object",
                        "properties": {
                            "f1": {
                                "type": "boolean"
                            }
                        }
                    },
                    {
                        "description": "second type",
                        "type": "number"
                    }
                ],
            }),
        );
    }
}
