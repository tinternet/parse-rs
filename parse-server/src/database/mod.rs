#[cfg(feature = "mongo-db")]
mod mongo;

#[cfg(feature = "postgres")]
mod postgres;

#[cfg(feature = "mongo-db")]
pub use mongo::Adapter;

#[cfg(feature = "postgres")]
pub use postgres::Database;
