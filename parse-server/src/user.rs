#[derive(Clone)]
pub struct User {
    pub id: Option<String>,
    pub application_id: Option<String>,
    pub installation_id: Option<String>,
    pub is_master: bool,
    pub is_read_only: bool,
    pub user: Option<String>,
    pub user_roles: Vec<String>,
    pub client_sdk: Option<String>,
}
