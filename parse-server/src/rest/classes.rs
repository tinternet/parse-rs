use actix_web::{post, web, HttpRequest, HttpResponse};
use bson::{Bson, Document, doc};

use crate::cache::AppCache;
use crate::database::DbAdapter;
use crate::error::Error;
use crate::operation::{execute, Context, FindRequest, Join, Request};
use crate::user::User;

fn parse_sort(payload: &Document) -> Option<Document> {
    let order = payload.get("order").and_then(|x| x.as_str()).unwrap_or("");
    if order == "" {
        None
    } else if &order[0..1] == "-" && &order[0..1] != "" {
        let mut doc = doc!{};
        doc.insert(&order[1..], -1);
        Some(doc)
    } else if &order[0..1] != "" {
        let mut doc = doc!{};
        doc.insert(order, 1);
        Some(doc)
    } else {
        None
    }
}

fn map_filter(key: &String, value: &Document) -> (String, Bson) {
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
            Bson::Document(value) => map_filter(key, value),
            _ => (key.clone(), value.clone()),
        })
        .fold(doc!{}, |mut doc, (key, value)| {
            doc.insert(key, value);
            doc
        });
    Some(filters)
}

fn map_join(doc: &Document) -> Result<Join, Error> {
    let key = doc.get("key").and_then(|x| x.as_str()).unwrap_or("");
    let empty = doc!{};
    let query = doc.get_document("key").unwrap_or(&empty);
    let class_name = query.get_str("className").unwrap_or("");
    let filter = query.get_document("where");

    if key == "" || class_name == "" {
        return Err(Error::BadFormat(format!("Improper usage of $select")));
    }

    Ok(Join {
        pointer_key: String::from(key),
        pointer_type: String::from(class_name),
        options: FindRequest {
            include: vec![],
            filter: None,
            limit: Some(1),
            skip: None,
            sort: None,
            join: vec![],
        },
    })
}

fn parse_joins(filter: &Document) -> Result<Vec<Join>, Error> {
    let mut result = Vec::new();
    let empty = doc!{};
    for join in filter
        .iter()
        .filter_map(|(_, value)| value.as_document())
        .filter_map(|doc| doc.get_document("$select").ok())
        .map(map_join)
    {
        match join {
            Ok(join) => {
                result.push(join);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
    Ok(result)
}

fn parse_user(payload: &Document) -> User {
    User {
        id: None,
        application_id: payload.get_str("_ApplicationId").ok().map(|x| x.to_string()),
        installation_id: payload.get_str("_InstallationId").ok().map(|x| x.to_string()),
        is_master: false,
        is_read_only: false,
        user: None,
        user_roles: vec![],
        client_sdk: payload.get_str("_ClientVersion").ok().map(|x| x.to_string()),
    }
}

fn parse_find_request(payload: &Document) -> Result<Request, Error> {
    Ok(Request::Find(FindRequest {
        filter: payload.get_document("where").ok().map(|x| x.clone()),
        include: payload.get_str("include").unwrap_or("").split(",").map(|x| x.to_string()).collect(),
        limit: payload.get_i64("limit").ok().map(|x| x.clone()),
        skip: payload.get_i64("skip").ok().map(|x| x.clone()),
        sort: parse_sort(&payload),
        join: parse_joins(&payload)?,
    }))
}

#[post("/parse/classes/{class_name}")]
pub async fn query_documents(
    db: web::Data<DbAdapter>,
    cache: web::Data<AppCache>,
    payload: String,
    _req: HttpRequest,
    class_name: web::Path<String>,
) -> HttpResponse {
    trace!("REST message IN: {}", &payload);

    let payload = match serde_json::from_str::<Document>(&payload) {
        Ok(map) => map,
        Err(e) => {
            let message = format!("Could not parse json request: {}", e.to_string());
            error!("{}", &message);
            return Error::BadFormat(message).to_http_response();
        }
    };

    let method = payload.get_str("_method").unwrap_or("");

    match method {
        "GET" => {
            let context = Context {
                class: class_name.to_string(),
                db: db.into_inner(),
                cache: cache.into_inner(),
                user: parse_user(&payload),
            };
            let request = match parse_find_request(&payload) {
                Ok(request) => request,
                Err(err) => return err.to_http_response(),
            };
            match execute(request, context).await {
                Ok(result) => HttpResponse::Ok().json(result),
                Err(err) => err.to_http_response(),
            }
        }
        _ => Error::BadFormat("".to_string()).to_http_response(),
    }

    // let allow_client_class_creation = true;
    // let is_master = false;
    // let is_system_class = false;
    // let class_name = class_name.to_string();
    // let auth = parse_auth(&request);
    // let operation = match request.method {
    //     Some(value) => OperationType::Find,
    //     None => OperationType::Create
    // };

    // let query = request.filter.map(|x| {
    //     match bson::Bson::try_from(x) {
    //         Ok(filter) => filter,
    //         Err(err) => {
    //             let message = format!("Could not parse filters: {}", err.to_string());
    //             error!("{}", &message);
    //             doc!{}
    //             // TODO:
    //             // return Error::BadFormat(message).to_http_response();
    //         }
    //     }
    // });
    // let operation = Operation{
    //     auth: parse_auth(&request),
    //     client_sdk: &request.client_version,
    //     class_name: &class_name,
    //     operation: operation,
    //     db: db.into_inner(),
    //     cache: cache.into_inner(),
    //     query: query
    // };

    // execute(operation).await?

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

// #[get("/parse/classes/{class_name}")]
// async fn asd() {

// }
