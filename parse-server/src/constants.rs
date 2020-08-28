use std::collections::HashSet;

lazy_static! {
    pub static ref MASTER_ONLY_ACCESS: HashSet<&'static str> = {
        let mut set = HashSet::new();
        set.insert("_JobStatus");
        set.insert("_PushStatus");
        set.insert("_Hooks");
        set.insert("_GlobalConfig");
        set.insert("_JobSchedule");
        set.insert("_Idempotency");
        set
    };
    pub static ref SYSTEM_CLASSES: HashSet<&'static str> = {
        let mut set = HashSet::new();
        set.insert("_User");
        set.insert("_Installation");
        set.insert("_Role");
        set.insert("_Session");
        set.insert("_Product");
        set.insert("_PushStatus");
        set.insert("_JobStatus");
        set.insert("_JobSchedule");
        set.insert("_Audience");
        set.insert("_Idempotency");
        set
    };
}
