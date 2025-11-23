use std::{fmt::Display, str::FromStr};

use anyhow::{anyhow, bail, Context};

pub trait MyCrc {
    fn new() -> Self;

    fn add(&mut self, buf: &[u8]);

    fn finalize(self) -> Crc;
}

impl MyCrc for crc_fast::Digest {
    fn new() -> Self {
        crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme)
    }

    fn add(&mut self, buf: &[u8]) {
        self.update(buf)
    }

    fn finalize(self) -> Crc {
        Crc(crc_fast::Digest::finalize(&self))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Crc(pub u64);

/// For now just as a decimal number
impl Display for Crc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "crc:{}", self.0)
    }
}

impl FromStr for Crc {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (prefix, digits) = s
            .split_once(':')
            .ok_or_else(|| anyhow!("expecting ':' in CRC string: {s:?}"))?;
        if prefix != "crc" {
            bail!("expecting 'crc:' prefix in CRC string: {s:?}")
        }
        Ok(Crc(digits.parse().with_context(|| {
            anyhow!("expecting u64 number after ':' in {s:?}")
        })?))
    }
}
