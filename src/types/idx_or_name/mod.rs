#[cfg(feature = "python")]
mod python;

/// A column index or name.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum IdxOrName {
    Idx(usize),
    Name(String),
}

impl IdxOrName {
    pub(crate) fn format_message(&self) -> String {
        match self {
            Self::Idx(idx) => format!("at index {idx}"),
            Self::Name(name) => format!("with name \"{name}\""),
        }
    }
}

impl From<usize> for IdxOrName {
    fn from(index: usize) -> Self {
        Self::Idx(index)
    }
}

impl From<String> for IdxOrName {
    fn from(name: String) -> Self {
        Self::Name(name)
    }
}

impl From<&str> for IdxOrName {
    fn from(name: &str) -> Self {
        Self::Name(name.to_owned())
    }
}
