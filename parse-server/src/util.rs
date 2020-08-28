use bson::Bson;
use std::collections::VecDeque;

fn find(mut path: VecDeque<&str>, value: &'static Bson) -> Option<&'static Bson> {
    let doc = match value {
        Bson::Document(doc) => doc,
        _ => return None,
    };
    let current_key = match path.pop_front() {
        Some(key) => key,
        None => return Some(value),
    };
    doc.get(current_key).and_then(|x| find(path, x))
}

pub fn r_get_str(path: Vec<&str>, value: &'static Bson) -> Option<&'static str> {
    let path = VecDeque::from(path);
    let object = find(path, value);
    object.and_then(|x| x.as_str())
}
