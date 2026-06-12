pub mod json;

use indexmap::IndexMap;

/// Represents a subset of [JSON Schema](https://json-schema.org/).
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub metadata: Metadata,
    pub kind: SchemaKind,
}

impl Schema {
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.metadata.title = Some(title.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.metadata.description = Some(description.into());
        self
    }

    pub fn string(rules: StringRules) -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::String(rules),
        }
    }

    pub fn number(rules: NumberRules) -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::Number(rules),
        }
    }

    pub fn integer(rules: NumberRules) -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::Integer(rules),
        }
    }

    pub fn boolean() -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::Boolean,
        }
    }

    pub fn null() -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::Null,
        }
    }

    pub fn object(rules: ObjectRules) -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::Object(Box::new(rules)),
        }
    }

    pub fn array(rules: ArrayRules) -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::Array(Box::new(rules)),
        }
    }

    pub fn union(variants: impl IntoIterator<Item = SchemaKind>) -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::Union(variants.into_iter().collect()),
        }
    }

    pub fn any_of(schemas: impl IntoIterator<Item = Schema>) -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::AnyOf(schemas.into_iter().collect()),
        }
    }

    pub fn all_of(schemas: impl IntoIterator<Item = Schema>) -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::AllOf(schemas.into_iter().collect()),
        }
    }

    pub fn one_of(schemas: impl IntoIterator<Item = Schema>) -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::OneOf(schemas.into_iter().collect()),
        }
    }

    pub fn not(schema: Schema) -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::Not(Box::new(schema)),
        }
    }

    pub fn any() -> Self {
        Self {
            metadata: Default::default(),
            kind: SchemaKind::Any,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Metadata {
    pub title: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SchemaKind {
    String(StringRules),
    Number(NumberRules),
    Integer(NumberRules),
    Boolean,
    Null,

    Object(Box<ObjectRules>),
    Array(Box<ArrayRules>),

    Union(Vec<SchemaKind>),

    AnyOf(Vec<Schema>),
    AllOf(Vec<Schema>),
    OneOf(Vec<Schema>),
    Not(Box<Schema>),

    Any,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct StringRules {
    pub min_length: Option<u64>,
    pub max_length: Option<u64>,
    pub pattern: Option<String>,
    pub format: Option<String>,
}

impl StringRules {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn min_length(mut self, min: u64) -> Self {
        self.min_length = Some(min);
        self
    }

    pub fn max_length(mut self, max: u64) -> Self {
        self.max_length = Some(max);
        self
    }

    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    pub fn format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct NumberRules {
    pub multiple_of: Option<f64>,
    pub minimum: Option<f64>,
    pub exclusive_minimum: Option<f64>,
    pub maximum: Option<f64>,
    pub exclusive_maximum: Option<f64>,
}

impl NumberRules {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn multiple_of(mut self, multiple: f64) -> Self {
        self.multiple_of = Some(multiple);
        self
    }

    pub fn minimum(mut self, min: f64) -> Self {
        self.minimum = Some(min);
        self
    }

    pub fn exclusive_minimum(mut self, min: f64) -> Self {
        self.exclusive_minimum = Some(min);
        self
    }

    pub fn maximum(mut self, max: f64) -> Self {
        self.maximum = Some(max);
        self
    }

    pub fn exclusive_maximum(mut self, max: f64) -> Self {
        self.exclusive_maximum = Some(max);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ArrayRules {
    pub items: Option<Box<Schema>>,
    pub min_items: Option<u64>,
    pub max_items: Option<u64>,
}

impl ArrayRules {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn items(mut self, schema: Schema) -> Self {
        self.items = Some(Box::new(schema));
        self
    }

    pub fn min_items(mut self, min: u64) -> Self {
        self.min_items = Some(min);
        self
    }

    pub fn max_items(mut self, max: u64) -> Self {
        self.max_items = Some(max);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ObjectRules {
    pub properties: IndexMap<String, Schema>,
    pub additional_properties: Option<bool>,
    pub required: Vec<String>,
    pub min_properties: Option<u64>,
    pub max_properties: Option<u64>,
}

impl ObjectRules {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn required_property(self, name: impl Into<String>, schema: Schema) -> Self {
        let name = name.into();
        self.property(name.clone(), schema).required(name)
    }

    pub fn property(mut self, name: impl Into<String>, schema: Schema) -> Self {
        self.properties.insert(name.into(), schema);
        self
    }

    pub fn additional_properties(mut self, allowed: bool) -> Self {
        self.additional_properties = Some(allowed);
        self
    }

    pub fn required(mut self, property: impl Into<String>) -> Self {
        self.required.push(property.into());
        self
    }

    pub fn min_properties(mut self, min: u64) -> Self {
        self.min_properties = Some(min);
        self
    }

    pub fn max_properties(mut self, max: u64) -> Self {
        self.max_properties = Some(max);
        self
    }
}
