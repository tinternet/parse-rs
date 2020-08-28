#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

pub use actix_web;
use actix_web::{middleware, web::Data, App, HttpServer};

mod database;
mod error;
mod schema;
mod user;
// mod api;
mod cache;
mod constants;
mod operation;
mod read;
mod util;
mod write;
// mod config;
// mod handlers;
mod rest;

// use schema::SchemaRepository;
// use document::DocumentRepository;

pub async fn run() -> std::io::Result<()> {
    let db = Data::new(database::DbAdapter::connect().await);
    let app_cache = Data::new(cache::AppCache::new());

    HttpServer::new(move || {
        App::new()
            .wrap(middleware::DefaultHeaders::new().header("X-Version", "0.2"))
            .wrap(middleware::Compress::default())
            .wrap(middleware::Logger::default())
            // .data(web::JsonConfig::default().limit(4096))
            .app_data(db.clone())
            .app_data(app_cache.clone())
            .service(rest::classes::query_documents)
    })
    .bind("127.0.0.1:5000")?
    .workers(8)
    .run()
    .await
}
