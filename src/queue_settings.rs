use std::default;

pub struct QueueSettings {
    pub delay_seconds: Option<u32>,
    pub max_size_bytes: Option<u32>,
    pub message_retention_period_seconds: Option<u32>,
    pub receive_wait_time_seconds: Option<u8>,
    pub visibility_timeout_seconds: Option<u32>,
}

impl default::Default for QueueSettings {
    fn default() -> Self {
        QueueSettings {
            delay_seconds: None,
            max_size_bytes: None,
            message_retention_period_seconds: None,
            receive_wait_time_seconds: None,
            visibility_timeout_seconds: None,
        }
    }
}
