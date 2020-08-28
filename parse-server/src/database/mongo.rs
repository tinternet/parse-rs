use bson::{Bson, Document};
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use futures::future::join_all;
use futures::stream::StreamExt;
use mongodb::bson::doc;
use mongodb::Database;
use mongodb::{options::ClientOptions, Client};
use std::collections::HashMap;
use std::sync::RwLockReadGuard;

use crate::cache::AppCache;
// use crate::document::{FindOptions, Join};
use crate::error::Error;
use crate::operation::{Operation, ReadOptions, Join};
use crate::schema::{Field, FieldType, Permissions, Schema};

#[derive(Clone)]
pub struct Adapter {
    db: Database,
}

impl From<mongodb::error::Error> for Error {
    fn from(error: mongodb::error::Error) -> Error {
        Error::Internal(error.to_string())
    }
}

impl Adapter {
    pub async fn connect() -> Adapter {
        let client_options = ClientOptions::parse("mongodb://192.168.254.111:27017/rust-parse")
            .await
            .unwrap();
        let client = Client::with_options(client_options).unwrap();
        Adapter {
            db: client.database("rust-parse"),
        }
    }

    pub async fn get_schema(&self) -> Result<HashMap<String, Schema>, Error> {
        let collection = self.db.collection("_SCHEMA");
        let mut cursor = collection.find(None, None).await?;
        let mut schemas = HashMap::new();

        while let Some(schema) = cursor.next().await {
            let schema = schema?;
            let id = schema.get_str("_id").unwrap_or("").to_string();
            schemas.insert(
                id.clone(),
                Schema {
                    name: id,
                    indexes: parse_indexes(&schema),
                    fields: parse_fields(&schema),
                    permissions: parse_permissions(&schema),
                },
            );
        }

        Ok(schemas)
    }

    pub async fn query_objects(
        &self,
        req: &Operation,
    ) -> Result<Vec<bson::Document>, Error> {
        let documents = query_objects(&self.db, req).await?;

        let futures = documents
            .into_iter()
            .map(|doc| join_pointers(&self.db, doc, req));

        Ok(join_all(futures)
            .await
            .into_iter()
            .filter_map(|x| x)
            .collect())
    }
}

fn bson_to_bool_map(value: Option<&bson::Bson>) -> HashMap<String, bool> {
    match value {
        Some(value) => {
            let empty = doc! {};
            let doc = value.as_document().unwrap_or(&empty);
            doc.iter().fold(HashMap::new(), |mut map, (key, val)| {
                map.insert(key.clone(), val.as_bool().unwrap_or(false));
                map
            })
        }
        None => HashMap::new(),
    }
}

fn parse_permissions(data: &bson::Document) -> Permissions {
    let empty = doc! {};
    let metadata = data.get_document("_metadata").unwrap_or(&empty);
    Permissions {
        add_field: bson_to_bool_map(metadata.get("addField")),
        count: bson_to_bool_map(metadata.get("count")),
        creat: bson_to_bool_map(metadata.get("create")),
        delete: bson_to_bool_map(metadata.get("delete")),
        find: bson_to_bool_map(metadata.get("find")),
        get: bson_to_bool_map(metadata.get("get")),
        protected_fields: bson_to_bool_map(metadata.get("protectedFields")),
        update: bson_to_bool_map(metadata.get("update")),
    }
}

fn parse_field(key: &String, field: &bson::Bson, fields_options: &bson::Document) -> Field {
    debug!("Parsing schema field: {}, {:?}", key, field);
    let empty = doc! {};
    let options = fields_options.get_document(key).unwrap_or(&empty);
    let required = options.get_bool("required").unwrap_or(false);
    let default_value = match options.get("defaultValue") {
        Some(v) => Some(v.clone()),
        None => None,
    };
    let (field_type, target_type) = match field.as_str().unwrap_or("") {
        "number" => (FieldType::Number, None),
        "string" => (FieldType::String, None),
        "boolean" => (FieldType::Boolean, None),
        "date" => (FieldType::Date, None),
        "map" => (FieldType::Object, None),
        "object" => (FieldType::Object, None),
        "array" => (FieldType::Array, None),
        "geopoint" => (FieldType::GeoPoint, None),
        "file" => (FieldType::File, None),
        "bytes" => (FieldType::Bytes, None),
        "polygon" => (FieldType::Polygon, None),
        field_type => {
            if &field_type[..1] == "*" {
                (FieldType::Pointer, Some(String::from(&field_type[1..])))
            } else if &field_type[..9] == "relation<" {
                (
                    FieldType::Relation,
                    Some(String::from(&field_type[9..field_type.len() - 1])),
                )
            } else {
                (FieldType::Unknown, None)
            }
        }
    };
    Field {
        name: key.clone(),
        field_type: field_type,
        required: required,
        target_type: target_type,
        default_value: default_value,
    }
}

fn parse_fields(data: &bson::Document) -> HashMap<String, Field> {
    debug!("Parsing schema fields");
    let empty = doc! {};
    let metadata = data.get_document("_metadata").unwrap_or(&empty);
    let fields_options = metadata.get_document("fields_options").unwrap_or(&empty);
    data.iter().fold(HashMap::new(), |mut map, (key, value)| {
        match key.as_str() {
            "_id" => {}       // ignore
            "_metadata" => {} // ignore
            _ => {
                map.insert(key.clone(), parse_field(key, value, fields_options));
            }
        };
        map
    })
}

fn parse_indexes(data: &bson::Document) -> HashMap<String, bool> {
    let empty = doc! {};
    let metadata = data.get_document("_metadata").unwrap_or(&empty);
    let indexes = metadata.get_document("indexes").unwrap_or(&empty);
    indexes
        .iter()
        .fold(HashMap::new(), |mut map, (key, value)| {
            map.insert(key.clone(), value.as_i32().unwrap_or(0) > 0);
            map
        })
}

fn parse_document(data: &bson::Document, schema: &Schema) -> bson::Document {
    let mut document = bson::Document::new();
    let schema = cache.schema.read().expect("RwLock poisoned");
    for (_, field) in &schema.fields {
        match field.field_type {
            FieldType::Pointer => {
                let value = data
                    .get_str(format!("_p_{}", &field.name).as_str())
                    .unwrap_or("");
                document.insert(
                    &field.name,
                    doc! {
                        "__type": "Pointer",
                        "className": field.target_type.as_ref().unwrap_or(&"".to_string()),
                        "objectId": value.split("$").nth(1).unwrap_or("")
                    },
                );
            }
            FieldType::Relation => {
                document.insert(
                    &field.name,
                    doc! {
                        "__type": "Relation",
                        "className": field.target_type.as_ref().unwrap_or(&"".to_string())
                    },
                );
            }
            FieldType::File => {
                let value = data.get_str(&field.name).unwrap_or("");
                document.insert(
                    &field.name,
                    doc! {
                        "__type": "File",
                        "name": value,
                        "url": format!("http://localhost:5000/parse/files/{}", value)
                    },
                );
            }
            _ => {
                let mut insert_if_value = |value: Option<&bson::Bson>| {
                    match value {
                        Some(value) => {
                            document.insert(&field.name, value);
                        }
                        None => {}
                    };
                };
                let mut insert_if_date = |value: Option<&DateTime<Utc>>| {
                    match value {
                        Some(date) => {
                            insert_if_value(Some(&bson::bson!(
                                date.to_rfc3339_opts(SecondsFormat::Millis, true)
                            )));
                        }
                        None => {}
                    };
                };
                match field.name.as_str() {
                    "objectId" => insert_if_value(data.get("_id")),
                    "createdAt" => insert_if_date(data.get_datetime("_created_at").ok()),
                    "updatedAt" => insert_if_date(data.get_datetime("_updated_at").ok()),
                    name => insert_if_value(data.get(name)),
                };
                match data.get(&field.name) {
                    Some(value) => {
                        match field.name.as_str() {
                            "objectId" => document.insert("_id", value),
                            "createdAt" => document.insert("_created_at", value),
                            "updatedAt" => document.insert("_updated_at", value),
                            name => document.insert(name, value),
                        };
                    }
                    None => {}
                };
            }
        };
    }
    document
}

async fn query_objects(db: &Database, req: &Operation) -> Result<Vec<bson::Document>, Error> {
    info!("Fetching objects for {}", req.class_name);

    let schema = match req.cache.get_schema(&req.class_name) {
        Some(schema) => schema,
        None => return Ok(vec![])
    };

    let default_options = ReadOptions::new();
    let read_options = req.read_options.as_ref().unwrap_or(&default_options);
    let collection = db.collection(&req.class_name);
    let find_options = mongodb::options::FindOptions::builder()
        .limit(read_options.limit)
        .sort(read_options.sort.clone())
        .skip(read_options.skip.clone())
        .build();

    let mut cursor = collection
        .find(read_options.filter.clone(), find_options)
        .await?;

    let mut results = Vec::new();

    while let Some(result) = cursor.next().await {
        match result {
            Ok(doc) => {
                let parsed = parse_document(&doc, schema);
                results.push(parsed);
            }
            Err(err) => {
                let err = Error::Internal(err.to_string());
                return Err(err);
            }
        }
    }

    Ok(results)

    // let results: Vec<Result<Document, mongodb::error::Error>> = cursor.collect().await;
    // let results = results.iter().filter_map(|v| match v {
    //     Ok(doc) => Some(parse_document(doc, schema.unwrap())),
    //     Err(_e) => None,
    // });

    // Ok(results.collect())
}

async fn join_pointers(
    db: &mongodb::Database,
    parent: Document,
    req: &Operation,
) -> Result<Document, Error> {
    let read_options = req.read_options.unwrap_or(ReadOptions::new());
    let schema = match req.cache.get_schema(&req.class_name) {
        Some(schema) => schema,
        None => return Ok(parent)
    };

    let joins: Vec<Join> = read_options
        .include
        .iter()
        .filter_map(|field| schema.fields.get(field))
        .filter(|field| field.target_type.is_some())
        .map(|field| Join {
            field_key: field.name.clone(),
            target_type: field.target_type.clone().unwrap(),
            filters: None,
        })
        .chain(read_options
            .join
            .iter()
            .filter_map(|j| schema.fields.get(&j.field_key).map(|s| (j, s)))
            .filter(|(j, s)| s.target_type.is_some())
            .filter(|(j, s)| j.target_type == s.target_type.unwrap()))
        .collect();

    let futures = joins
        .iter()
        .map(|join| join_pointer(db, join, &parent, schema, req));

    match join_all(futures).await.iter().filter_map(|x| x.err()).nth(0) {
        Some(err) => Err(err),
        None => Ok(parent)
    }
}

async fn join_pointer(
    db: &mongodb::Database,
    join: Join,
    parent: &Document,
    source_schema: &Schema,
    req: &Operation,
) -> Result<(), Error> {
    debug!(
        "Joining pointer to {} for key {}",
        join.target_type, join.field_key
    );

    match field.field_type {
        FieldType::Pointer => {
            debug!("Fetching pointer for {FindOptions}", join.field_key);
            let empty = doc! {};
            let id = parent
                .get_document(&join.field_key)
                .unwrap_or(&empty)
                .get_str("objectId")
                .unwrap_or("");
            if id == "" {
                return None;
            }
            let opts =  {
                filter: join.filters.clone().map(|mut filters| {
                    filters.insert("_id", id);
                    filters
                }),
                limit: Some(1),
                skip: None,
                sort: None,
                join: Vec::new(),
                include: Vec::new(),
            };
            match find_objects(db, target_type.as_str(), &opts, cache).await {
                Ok(mut results) => {
                    debug!(
                        "Joined pointer: {} = {} #{}, {}",
                        join.field_key,
                        join.target_type,
                        id,
                        results.len()
                    );
                    results.pop().map(|x| (join.field_key.clone(), x))
                }
                Err(e) => {
                    error!("Could not join pointer: {}", e.to_string());
                    None
                }
            }
        }
        _ => None,
    }
}
