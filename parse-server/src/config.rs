const CONFIG: Config = Config {
    allow_client_class_creation: false,
};

#[derive(Debug)]
pub struct Config {
    pub allow_client_class_creation: bool,
}

pub fn config() -> Config {
    CONFIG
}

// pub fn init() {

// }
