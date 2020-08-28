use actix_web::HttpResponse;

#[derive(Clone)]
pub enum Error {
    Internal(String),
    NotFound(String),
    BadFormat(String),
    Forbidden(String),
}

#[inline]
fn format_json(code: i32, message: &String) -> String {
    format!(r#"{{"code":{},"message": "{}"}}"#, code, message)
}

impl Error {
    pub fn to_json(&self) -> String {
        match self {
            Error::Internal(message) => format_json(100, message),
            Error::NotFound(message) => format_json(101, message),
            Error::BadFormat(message) => format_json(102, message),
            Error::Forbidden(message) => format_json(119, message),
        }
    }

    pub fn to_http_response(&self) -> HttpResponse {
        match self {
            Error::Internal(message) => HttpResponse::BadRequest().body(self.to_json()),
            Error::NotFound(message) => HttpResponse::NotFound().body(self.to_json()),
            Error::BadFormat(message) => HttpResponse::BadRequest().body(self.to_json()),
            Error::Forbidden(message) => HttpResponse::BadRequest().body(self.to_json()),
        }
    }
}

impl ToString for Error {
    fn to_string(&self) -> String {
        match &self {
            Error::Internal(message) => format!("Internal Error: {}", message),
            Error::NotFound(message) => format!("Not Found: {}", message),
            Error::BadFormat(message) => format!("Bad Format: {}", message),
            Error::Forbidden(message) => format!("Permission Denied: {}", message),
        }
    }
}
