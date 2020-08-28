use crate::cache::AppCache;
use crate::constants::{MASTER_ONLY_ACCESS, SYSTEM_CLASSES};
use crate::database::DbAdapter;
use crate::error::Error;
use crate::read::read;
use crate::user::User;
use crate::write::write;
use bson::Document;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct Context {
    pub class: String,
    pub user: User,
    pub db: Arc<DbAdapter>,
    pub cache: Arc<AppCache>,
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

pub struct FindRequest {
    pub include: Vec<String>,
    pub filter: Option<Document>,
    pub limit: Option<i64>,
    pub skip: Option<i64>,
    pub sort: Option<Document>,
    pub join: Vec<Join>,
}

pub struct CreateRequest {
    pub params: Document,
}

pub struct UpdateRequest {
    pub objectId: String,
    pub params: Document,
}

pub struct DeleteRequest {
    pub objectId: String,
}

pub struct Join {
    pub pointer_key: String,
    pub pointer_type: String,
    pub options: FindRequest,
}

pub struct Relation {
    pub relation_key: String,
    pub relation_type: String,
    pub filters: Option<Document>,
}

// TODO: fix error handling
async fn fetch_schema(ctx: &Context) -> Result<(), Error> {
    if ctx.cache.schema_loaded.load(Ordering::Relaxed) {
        info!("Using cached schema!");
        return Ok(());
    }

    info!("Loading schema...");
    let schema = ctx.db.get_schema().await?;
    info!("Schema loaded! count: {}", schema.len());

    *ctx.cache.schema.write().expect("RwLock is poisoned") = schema;
    ctx.cache.schema_loaded.store(true, Ordering::Relaxed);

    Ok(())
}

// TODO: fix error handling
fn validate_class_creation(ctx: &Context) -> Result<(), Error> {
    let allow_client_class_creation = false;

    if !allow_client_class_creation
        && !ctx.user.is_master
        && !SYSTEM_CLASSES.contains(ctx.class.as_str())
        && ctx.cache.get_schema(&ctx.class).is_none()
    {
        let message = format!(
            "This user is not allowed to access non-existent class: {}",
            ctx.class
        );
        Err(Error::Forbidden(message))
    } else {
        Ok(())
    }
}

// TODO: fix error handling
fn enforce_role_security(req: &Request, ctx: &Context) -> Result<(), Error> {
    if ctx.class == "_Installation" && !ctx.user.is_master {
        match req {
            Request::Delete(_) | Request::Find(_) => {
                let message = format!("Clients aren't allowed to perform the {} operation on the installation collection.", "ctx.request");
                error!("{}", message);
                return Err(Error::Forbidden(message));
            }
            _ => {}
        }
    }

    if !ctx.user.is_master && MASTER_ONLY_ACCESS.contains(ctx.class.as_str()) {
        let message = format!(
            "Clients aren't allowed to perform the {} operation on the {} collection.",
            "ctx.operation", ctx.class
        );
        error!("{}", message);
        return Err(Error::Forbidden(message));
    }

    match req {
        Request::Delete(_) | Request::Create(_) | Request::Update(_) => {
            if ctx.user.is_read_only {
                let message = format!(
                    "read-only masterKey isn't allowed to perform the {} operation.",
                    "req.operation"
                );
                error!("{}", message);
                return Err(Error::Forbidden(message));
            }
        }
        _ => {}
    }

    Ok(())
}

pub async fn execute(req: Request, ctx: Context) -> Result<(), Error> {
    info!("Executing read for {}", ctx.class);
    fetch_schema(&ctx).await?;
    validate_class_creation(&ctx)?;
    enforce_role_security(&req, &ctx)?;

    match req {
        Request::Get(_) => read(req, ctx).await,
        Request::Find(_) => read(req, ctx).await,
        Request::Create(_) => read(req, ctx).await,
        Request::Update(_) => write(req, ctx).await,
        Request::Delete(_) => write(req, ctx).await,
    }
}
