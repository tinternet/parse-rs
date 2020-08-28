use crate::database::Database;
use crate::error::Error;
use crate::schema::Schema;
use bson::Document;
use std::collections::HashMap;
use std::sync::RwLockReadGuard;

#[derive(Clone)]
pub struct DocumentRepository {
    db: Database,
}

#[derive(Debug, Clone)]
pub struct Join {
    pub field_key: String,
    pub target_type: String,
    pub filters: Option<Document>,
}

#[derive(Debug, Clone)]
pub struct FindOptions {
    pub filter: Option<Document>,
    pub limit: Option<i64>,
    pub skip: Option<i64>,
    pub sort: Option<Document>,
    pub join: Vec<Join>,
    pub include: Vec<String>,
}

struct Context {
    acl: Vec<String>,
}

const SYSTEM_TYPES: &'static [&'static str] = &[
    "_User",
    "_Installation",
    "_Role",
    "_Session",
    "_Product",
    "_PushStatus",
    "_JobStatus",
    "_JobSchedule",
    "_Audience",
    "_Idempotency",
];

impl DocumentRepository {
    pub fn new(db: Database) -> Self {
        DocumentRepository { db }
    }

    pub async fn find_objects(
        &self,
        class_name: &str,
        mut options: FindOptions,
        schema: RwLockReadGuard<'_, HashMap<String, Schema>>,
    ) -> Result<Vec<Document>, Error> {
        info!("Querying objects for {}", class_name);

        if options.limit.is_none() {
            options.limit = Some(100);
        }

        let mut context = Context { acl: vec![] };

        // TODO: get values
        let is_master = false;
        let user = false;
        let redirect_key = false;
        let allow_client_class_creation = true;

        if !is_master {
            context.acl = vec!["*".to_string()];
        }
        if user {
            // TODO: get user auth
        }

        if redirect_key {
            // TODO: redirect
        }

        if !allow_client_class_creation
            && !is_master
            && !SYSTEM_TYPES.iter().any(|&t| t == class_name)
            && !schema.contains_key(class_name)
        {
            let message = format!(
                "This user is not allowed to access non-existent class: {}",
                class_name
            );
            return Err(Error::Internal(message));
        }

        // TODO: check select RestQuery.js 451
        // TODO: check RestQuery.js 503

        // TODO: replace in query RestQuery.js 331

        let objects = self.db.query_objects(class_name, options, schema).await?;
        let objects = objects.iter().map(|object| object.clone());
        let objects: Vec<bson::Document> = objects.collect();

        // info!("Query returned {} objects", objects.len());
        Ok(objects)
    }
}
