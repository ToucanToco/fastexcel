use pyo3::PyAny;

use crate::error::{FastExcelError, FastExcelErrorKind, FastExcelResult};

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

impl TryFrom<&PyAny> for IdxOrName {
    type Error = FastExcelError;

    fn try_from(py_any: &PyAny) -> FastExcelResult<Self> {
        if let Ok(name) = py_any.extract::<String>() {
            Ok(IdxOrName::Name(name))
        } else if let Ok(index) = py_any.extract::<usize>() {
            Ok(IdxOrName::Idx(index))
        } else {
            Err(FastExcelErrorKind::InvalidParameters(format!(
                "cannot create IdxOrName from {py_any:?}"
            ))
            .into())
        }
    }
}
