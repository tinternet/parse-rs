use mongodb::bson::Bson;
use std::collections::HashMap;

#[derive(Clone)]
pub enum FieldType {
    Number,
    String,
    Boolean,
    Date,
    Object,
    Array,
    GeoPoint,
    File,
    Bytes,
    Polygon,
    Pointer,
    Relation,
    Unknown,
}

#[derive(Clone)]
pub struct Schema {
    pub name: String,
    pub permissions: Permissions,
    pub indexes: HashMap<String, bool>,
    pub fields: HashMap<String, Field>,
}

#[derive(Clone)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub target_type: Option<String>,
    pub required: bool,
    pub default_value: Option<Bson>,
}

#[derive(Clone)]
pub struct Permissions {
    pub add_field: HashMap<String, bool>,
    pub count: HashMap<String, bool>,
    pub creat: HashMap<String, bool>,
    pub delete: HashMap<String, bool>,
    pub find: HashMap<String, bool>,
    pub get: HashMap<String, bool>,
    pub protected_fields: HashMap<String, bool>,
    pub update: HashMap<String, bool>,
}

impl Permissions {
    pub fn new() -> Self {
        Permissions {
            add_field: HashMap::new(),
            count: HashMap::new(),
            creat: HashMap::new(),
            delete: HashMap::new(),
            find: HashMap::new(),
            get: HashMap::new(),
            protected_fields: HashMap::new(),
            update: HashMap::new(),
        }
    }
}

impl Schema {
    pub fn new(name: String) -> Self {
        Schema {
            name: name,
            permissions: Permissions::new(),
            indexes: HashMap::new(),
            fields: HashMap::new(),
        }
    }
}
