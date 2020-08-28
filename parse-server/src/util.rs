use bson::Document;
use bson::Bson;

pub fn get_key<'a>(path: &str, doc: &'a Document) -> Option<&'a Bson> {
    let mut current_doc = doc;
    let mut result = None;
    let keys: Vec<&str> = path.split(".").collect();

    for key in keys {
        match doc.get_document(key) {
            Ok(doc) => {
                result = doc.get(key);
                current_doc = doc;
            }
            Err(_) => {
                return None;
            }
        }
    }

    result
}

pub fn get_str<'a>(path: &str, doc: &'a Document) -> Option<&'a str> {
    match get_key(path, doc) {
        Some(x) => x.as_str(),
        None => None
    }
}
