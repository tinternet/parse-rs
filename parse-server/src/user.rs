pub struct User {
    pub id: String,
    pub application_id: String,
    pub installation_id: String,
    pub is_master: bool,
    pub is_read_only: bool,
    pub user: Option<String>,
    pub user_roles: Vec<String>,
    pub client_sdk: String,
}
