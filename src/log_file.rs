use std::{
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

pub struct UnixTimeWrap(pub SystemTime);

impl Display for UnixTimeWrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let seconds = self
            .0
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime::now is always within range")
            .as_secs_f64();
        write!(f, "{seconds}")
    }
}
