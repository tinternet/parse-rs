use actix_web::{get, post, web, HttpRequest, HttpResponse};
use bson::{doc, Bson, Document};

use serde::Deserialize;
use std::convert::TryFrom;
use std::sync::atomic::Ordering;

use crate::cache::AppCache;
// use crate::database::Database;
// use crate::document::{DocumentRepository, FindOptions, Join};
use crate::error::Error;
// use crate::schema::SchemaRepository;
// use crate::auth::Auth;
use crate::read::read;
use crate::write::write;
use crate::operation::{execute, Operation, OperationType};

#[derive(Deserialize)]
struct Request {
    #[serde(rename(deserialize = "_ApplicationId"))]
    application_id: String,

    #[serde(rename(deserialize = "_ClientVersion"))]
    client_version: String,
    #[serde(rename(deserialize = "_InstallationId"))]
    installation_id: String,
    #[serde(rename(deserialize = "_method"))]
    method: Option<String>,
    #[serde(rename(deserialize = "where"))]
    filter: Option<serde_json::Value>,

    include: Option<String>,
    limit: Option<i64>,
    skip: Option<i64>,
    order: Option<String>,
}

const MASTER_ONLY: &'static [&'static str] = &[
    "JobStatus",
    "PushStatus",
    "Hooks",
    "GlobalConfig",
    "JobSchedule",
    "Idempotency",
];

fn parse_sort(order: String) -> Document {
    if &order[0..1] == "-" {
        doc! {
            &order[1..]: -1
        }
    } else {
        doc! {
            &order: 1
        }
    }
}

fn map_filters(key: &String, value: &Document) -> (String, Bson) {
    let mut new_value = value.clone();
    let mut has_direct_constraint = false;
    let mut has_operator_constraint = false;
    let mut equality = doc! {};

    for (key, value) in value.iter() {
        if &key[..1] == "$" {
            has_operator_constraint = true;
        } else {
            has_direct_constraint = true;
            equality.insert(key, value);
        }
    }

    if has_direct_constraint && has_operator_constraint {
        new_value.insert("$eq", &equality);
        for (key, _) in equality.iter() {
            new_value.remove(key);
        }
    }

    new_value.remove("$select");
    (key.clone(), bson::bson!(new_value))
}

fn parse_filters(filter: &Document) -> Option<Document> {
    if filter.len() == 0 {
        return None;
    }
    let filters = filter
        .into_iter()
        .map(|(key, value)| match value {
            Bson::Document(value) => map_filters(key, value),
            _ => (key.clone(), value.clone()),
        })
        .fold(doc! {}, |mut doc, (key, value)| {
            doc.insert(key, value);
            doc
        });
    Some(filters)
}

fn parse_joins(filter: &Document) -> Vec<Result<Join, Error>> {
    filter
        .iter()
        .filter_map(|(key, value)| value.as_document())
        .filter(|doc| doc.contains_key("$select"))
        .map(|doc| {
            let empty = doc! {};
            let select = doc.get_document("$select").unwrap_or(&empty);
            let key = select.get_str("key").unwrap_or("");
            let query = select.get_document("query").unwrap_or(&empty);
            let class_name = query.get_str("className").unwrap_or("");
            let filter = query.get_document("where");

            if key == "" || class_name == "" {
                return Err(Error::BadFormat(format!("Improper usage of $select")));
            }

            Ok(Join {
                field_key: String::from(key),
                target_type: String::from(class_name),
                filters: filter.ok().map(|f| f.clone()),
            })
        })
        .collect()
}

fn check_role_security(method: &str, class_name: &str, auth: &Auth) -> Result<(), Error> {
    if class_name == "_Installation" && !auth.is_master {
        if method == "delete" || method == "find" {
            let message = format!("Clients aren't allowed to perform the {} operation on the installation collection.", method);
            error!("{}", message);
            return Err(Error::Forbidden(message));
        }
    } //all volatileClasses are masterKey only

    if MASTER_ONLY.iter().find(|x| **x == class_name).is_some() && !auth.is_master {
        let message = format!(
            "Clients aren't allowed to perform the {} operation on the {} collection.",
            method, class_name
        );
        error!("{}", message);
        return Err(Error::Forbidden(message));
    } // readOnly masterKey is not allowed

    if auth.is_read_only && (method == "delete" || method == "create" || method == "update") {
        let message = format!(
            "read-only masterKey isn't allowed to perform the {} operation.",
            method
        );
        error!("{}", message);
        return Err(Error::Forbidden(message));
    }

    Ok(())
}

fn parse_auth(request: &Request) -> Auth {
    Auth{
        application_id: request.application_id,
        installation_id: request.installation_id,
        is_master: false,
        is_read_only: false,
        user: None,
        user_roles: Vec::new()
    }
}


#[post("/parse/classes/{class_name}")]
pub async fn query_documents(
    db: web::Data<Database>,
    cache: web::Data<AppCache>,
    payload: String,
    _req: HttpRequest,
    class_name: web::Path<String>,
) -> HttpResponse {
    trace!("REST message IN: {}", &payload);

    // Try to parse request
    let request = match serde_json::from_str::<Request>(&payload) {
        Ok(request) => request,
        Err(e) => {
            let message = format!("Could not parse json request: {}", e.to_string());
            error!("{}", &message);
            return Error::BadFormat(message).to_http_response();
        }
    };

    let allow_client_class_creation = true;
    let is_master = false;
    let is_system_class = false;
    let class_name = class_name.to_string();

    let auth = parse_auth(&request);
    let operation = match request.method {
        Some(value) => OperationType::Find,
        None => OperationType::Create
    };

    let query = request.filter.map(|x| {
        match bson::Bson::try_from(x) {
            Ok(filter) => filter,
            Err(err) => {
                let message = format!("Could not parse filters: {}", err.to_string());
                error!("{}", &message);
                doc!{}
                // TODO:
                // return Error::BadFormat(message).to_http_response();
            }
        }
    });
    
    let operation = Operation{
        auth: parse_auth(&request),
        client_sdk: &request.client_version,
        class_name: &class_name,
        operation: operation,
        db: db.into_inner(),
        cache: cache.into_inner(),
        query: query
    };

    execute(operation).await?

    // match method {
    //     "find" => {
    //         let request = ReadRequest{
    //         };
    //         match read(request).await {
    //             Ok(_) => {},
    //             Err(_) => {}
    //         }
    //     }
    //     "create" => {
    //         let request = WriteRequest{
    //             auth: parse_auth(&request),
    //             client_sdk: &request.client_version,
    //             class_name: &class_name,
    //             db: db.into_inner(),
    //             cache: cache.into_inner(),
    //             query: Some(query),
    //             data: doc!{}
    //         };
    //         match write(request).await {
    //             Ok(_) => {},
    //             Err(_) => {}
    //         }
    //     }
    // }

    

    match check_role_security(method, &class_name, &auth) {
        Ok(()) => {},
        Err(e) => return e.to_http_response()
    }

    

    // Validate class creation
    if !allow_client_class_creation
        && !is_master
        && !is_system_class
        && !schema.contains_key(&class_name)
    {
        let message = format!(
            "This user is not allowed to access non-existent class: {}",
            class_name
        );
        return Error::Forbidden(message).to_http_response();
    }

    if !schema.contains_key(&class_name) {
        let results: Vec<String> = Vec::new();
        return HttpResponse::Ok().json(doc! {
            "results": results
        });
    }

    

    // if request.method != "GET" {
    //     match check_role_security("create", &class_name) {
    //         Ok(()) => {},
    //         Err(e) => return e.to_http_response()
    //     }
    // }

    // // Try parse filters
    // let (filter, join) = match request.filter {
    //     Some(filter) => {
    //         let filter = match bson::Bson::try_from(filter) {
    //             Ok(filter) => filter,
    //             Err(err) => {
    //                 let message = format!("Could not parse filters: {}", err.to_string());
    //                 error!("{}", &message);
    //                 return Error::BadFormat(message).to_http_response();
    //             }
    //         };
    //         match filter.as_document() {
    //             Some(doc) => (parse_filters(doc), parse_joins(doc)),
    //             None => (None, Vec::new()),
    //         }
    //     }
    //     None => (None, Vec::new()),
    // };

    // // Check for join payload errors
    // match join.iter().filter_map(|val| val.as_ref().err()).nth(0) {
    //     Some(err) => return err.to_http_response(),
    //     None => {}
    // }

    // // TODO: Check get user data

    // // TODO: redirect class name for key

    // // TODO: dont select

    // // TODO: in query

    // // TODO: not in query

    // let include_all = false;

    // // TODO: 654 restquery.js
    // if include_all {}

    // let options = FindOptions {
    //     sort: request.order.map(parse_sort),
    //     filter: filter,
    //     limit: request.limit,
    //     skip: request.skip,
    //     join: join
    //         .iter()
    //         .filter_map(|val| val.as_ref().ok())
    //         .map(|val| val.clone())
    //         .collect(),
    //     include: request
    //         .include
    //         .unwrap_or("".to_string())
    //         .split(",")
    //         .map(|x| x.to_string())
    //         .collect(),
    // };

    // match document_repository
    //     .find_objects(&class_name, options, schema)
    //     .await
    // {
    //     Ok(result) => HttpResponse::Ok().json(doc! {
    //         "results": result
    //     }),
    //     Err(e) => e.to_http_response(),
    // }

    // let class = class.into_inner();
    // class.find_schema("asdsadasda").await;

    // class.into_inner().clone().find_schema("asdasdasd").await;

    // let request = match serde_json::from_str::<Request>(&item) {
    //     Ok(request) => request,
    //     Err(e) => return HttpResponse::Ok().body(e.to_string())
    // };

    // HttpResponse::Ok().body("not found")

    // let query = handlers::Query{
    //     auth: handlers::Auth{installation_id: request.installation_id, is_master: false, user: None},
    //     class_name: name.as_str().to_string(),
    //     client_sdk: handlers::ClientSdk{language: String::from(""), version: String::from("")},
    //     do_count: false,
    //     find_options: handlers::QueryOptions{limit: 100, acl: vec![]},
    //     include: vec![],
    //     include_all: false,
    //     rest_options: handlers::QueryOptions{limit: 100, acl: vec![]},
    //     rest_where: std::collections::HashMap::new(),
    //     run_after_find: false
    // };

    // match handlers::classes::find(query, db.into_inner()).await {
    //     Ok(result) => HttpResponse::Ok().json(result),
    //     Err(e) => match e {
    //         handlers::Error::Internal(e) => HttpResponse::Ok().body(e),
    //         handlers::Error::NotFound() => HttpResponse::Ok().body("not found")
    //     }
    // }
}
