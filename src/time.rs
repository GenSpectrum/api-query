use std::{
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Local};

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

pub fn system_time_to_rfc3339(t: SystemTime) -> String {
    let t: DateTime<Local> = DateTime::from(t);
    t.to_rfc3339()
}

pub struct Rfc3339TimeWrap(pub SystemTime);

impl Display for Rfc3339TimeWrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&system_time_to_rfc3339(self.0))
    }
}
