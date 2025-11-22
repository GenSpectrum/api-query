use std::{
    fmt::Display,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context};
use chrono::{DateTime, Local};

#[derive(Debug)]
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

impl FromStr for UnixTimeWrap {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let x: f64 = s
            .parse()
            .with_context(|| anyhow!("expecting real number for unixtime"))?;
        let d = Duration::from_secs_f64(x);
        let t = UNIX_EPOCH
            .checked_add(d)
            .with_context(|| anyhow!("real number is not valid as unixtime: {x}"))?;
        Ok(Self(t))
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
