// use mongodb::{Client, options::ClientOptions};
// use actix_web::{get, middleware, post, web, App, HttpRequest, HttpResponse, HttpServer};
// use serde::{Deserialize, Serialize};

// mod api;
// mod handlers;
// mod config;
// mod database;

#[parse_server::actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var(
        "RUST_LOG",
        "actix_server=info,actix_web=info,parse_server=trace",
    );
    env_logger::init();
    parse_server::run().await
}
