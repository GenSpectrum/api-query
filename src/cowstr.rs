/// Because Cow<str> does not support AsRef<[u8]>:
pub enum Cowstr<'t> {
    Str(&'t str),
    String(String),
}

impl<'t> From<String> for Cowstr<'t> {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl<'t> From<&'t str> for Cowstr<'t> {
    fn from(value: &'t str) -> Self {
        Self::Str(value)
    }
}

impl<'t> AsRef<str> for Cowstr<'t> {
    fn as_ref(&self) -> &str {
        match self {
            Cowstr::Str(s) => s,
            Cowstr::String(s) => s,
        }
    }
}

impl<'t> AsRef<[u8]> for Cowstr<'t> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Cowstr::Str(s) => s.as_bytes(),
            Cowstr::String(s) => s.as_bytes(),
        }
    }
}
