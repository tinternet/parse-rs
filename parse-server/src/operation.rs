use crate::cache::AppCache;
use crate::constants::{MASTER_ONLY_ACCESS, SYSTEM_CLASSES};
use crate::database::Adapter;
use crate::error::Error;
use crate::user::User;
use bson::Document;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct Operation {
    pub class: String,
    pub user: User,
    pub db: Arc<Adapter>,
    pub cache: Arc<AppCache>,
    pub request: Request,
}

pub enum Request {
    Get(GetRequest),
    Find(FindRequest),
    Create(CreateRequest),
    Update(UpdateRequest),
    Delete(DeleteRequest),
}

pub struct GetRequest {
    pub objectId: String,
    pub join: Vec<Join>,
}

pub struct FindOptions {
    pub filter: Option<Document>,
    pub limit: Option<i64>,
    pub skip: Option<i64>,
    pub sort: Option<Document>,
    pub join: Vec<Join>,
    pub include: Vec<String>,
}

pub struct FindRequest {
    pub filters: Option<Document>,
    pub options: FindOptions,
}

pub struct CreateRequest {
    pub params: Document,
}

pub struct UpdateRequest {
    pub objectId: String,
    pub params: Document,
}

pub struct DeleteRequest {
    pub user: User,
    pub db: Arc<Adapter>,
    pub cache: Arc<AppCache>,
    pub objectId: String,
}

pub struct Join {
    pub pointer_key: String,
    pub pointer_type: String,
    pub objectId: String,
    pub options: FindOptions,
}

pub struct Relation {
    pub relation_key: String,
    pub relation_type: String,
    pub filters: Option<Document>,
}

// TODO: fix error handling
async fn fetch_schema(req: &Operation) -> Result<(), Error> {
    if req.cache.schema_loaded.load(Ordering::Relaxed) {
        info!("Using cached schema!");
        return Ok(());
    }

    info!("Loading schema...");
    let schema = req.db.get_schema().await?;
    info!("Schema loaded! count: {}", schema.len());

    *req.cache.schema.write().expect("RwLock is poisoned") = schema;
    req.cache.schema_loaded.store(true, Ordering::Relaxed);

    Ok(())
}

// TODO: fix error handling
fn validate_class_creation(req: &Operation) -> Result<(), Error> {
    let allow_client_class_creation = false;

    if !allow_client_class_creation
        && !req.user.is_master
        && !SYSTEM_CLASSES.contains(&req.class)
        && req.cache.get_schema(&req.class).is_none()
    {
        let message = format!(
            "This user is not allowed to access non-existent class: {}",
            req.class
        );
        Err(Error::Forbidden(message))
    } else {
        Ok(())
    }
}

// TODO: fix error handling
fn enforce_role_security(req: &Operation) -> Result<(), Error> {
    if req.class == "_Installation" && !req.user.is_master {
        match req.request {
            Request::Delete(_) | Request::Find(_) => {
                let message = format!("Clients aren't allowed to perform the {:?} operation on the installation collection.", req.request);
                error!("{}", message);
                return Err(Error::Forbidden(message));
            }
            _ => {}
        }
    }

    if !req.user.is_master && MASTER_ONLY_ACCESS.contains(&req.class)  {
        let message = format!(
            "Clients aren't allowed to perform the {} operation on the {} collection.",
            req.operation, req.class_name
        );
        error!("{}", message);
        return Err(Error::Forbidden(message));
    }

    if req.auth.is_read_only
        && (req.operation == OperationType::Delete
            || req.operation == OperationType::Create
            || req.operation == OperationType::Update)
    {
        let message = format!(
            "read-only masterKey isn't allowed to perform the {} operation.",
            req.operation
        );
        error!("{}", message);
        return Err(Error::Forbidden(message));
    }

    Ok(())
}

pub async fn execute<T>(op: &Operation<T>) -> Result<(), Error> {
    info!("Executing read for {}", op.class);
    fetch_schema(op).await?;
    validate_class_creation(op)?;
    enforce_role_security(op)?;

    match op.request {
        Request::Get(_) => crate::read::read(op).await,
        Request::Find(_) => crate::read::read(op).await,
        Request::Create(_) => crate::read::read(op).await,
        Request::Update(_) => crate::read::write(op).await,
        Request::Delete(_) => crate::read::write(op).await,
    }
}
