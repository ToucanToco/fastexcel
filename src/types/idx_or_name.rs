#[derive(Debug)]
pub(crate) enum IdxOrName {
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
