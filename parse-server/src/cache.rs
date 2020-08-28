use std::sync::{RwLock, RwLockReadGuard, Arc};
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::collections::HashMap;
use crate::schema;
use crate::user::User;

pub type Schema = HashMap<String, schema::Schema>;
pub type Sessions = HashMap<String, User>;

pub struct AppCache {
    pub schema: RwLock<Schema>,
    pub schema_loaded: AtomicBool,
    pub sessions: RwLock<Sessions>,
}

impl AppCache {
    pub fn new() -> Self {
        AppCache {
            schema: RwLock::new(HashMap::new()),
            schema_loaded: AtomicBool::from(false),
            sessions: RwLock::new(HashMap::new())
        }
    }

    pub fn get_schema(&self, class_name: &str) -> Option<schema::Schema> {
        let schema = self.schema.read().expect("RwLock poisoned");
        let value = schema.get(class_name);
        value.map(|x| x.clone())
    }
}
