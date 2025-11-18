use std::fmt::Display;

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

pub struct Crc(u64);

/// For now just as a decimal number
impl Display for Crc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
