use crate::types::idx_or_name::IdxOrName;
use calamine::XlsxError;
use std::{error::Error, fmt::Display};

/// The kind of a fastexcel error.
#[derive(Debug)]
pub enum FastExcelErrorKind {
    UnsupportedColumnTypeCombination(String),
    CannotRetrieveCellData(usize, usize),
    CalamineCellError(calamine::CellErrorType),
    CalamineError(calamine::Error),
    SheetNotFound(IdxOrName),
    ColumnNotFound(IdxOrName),
    // Arrow errors can be of several different types (arrow::error::Error, PyError), and having
    // the actual type has not much value for us, so we just store a string context
    ArrowError(String),
    InvalidParameters(String),
    InvalidColumn(String),
    Internal(String),
}

impl Display for FastExcelErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FastExcelErrorKind::UnsupportedColumnTypeCombination(detail) => {
                write!(f, "unsupported column type combination: {detail}")
            }
            FastExcelErrorKind::CannotRetrieveCellData(row, col) => {
                write!(f, "cannot retrieve cell data at ({row}, {col})")
            }
            FastExcelErrorKind::CalamineCellError(calamine_error) => {
                write!(f, "calamine cell error: {calamine_error}")
            }
            FastExcelErrorKind::CalamineError(calamine_error) => {
                write!(f, "calamine error: {calamine_error}")
            }
            FastExcelErrorKind::SheetNotFound(idx_or_name) => {
                let message = idx_or_name.format_message();
                write!(f, "sheet {message} not found")
            }
            FastExcelErrorKind::ColumnNotFound(idx_or_name) => {
                let message = idx_or_name.format_message();
                write!(f, "column {message} not found")
            }
            FastExcelErrorKind::ArrowError(err) => write!(f, "arrow error: {err}"),
            FastExcelErrorKind::InvalidParameters(err) => write!(f, "invalid parameters: {err}"),
            FastExcelErrorKind::InvalidColumn(err) => write!(f, "invalid column: {err}"),
            FastExcelErrorKind::Internal(err) => write!(f, "fastexcel error: {err}"),
        }
    }
}

/// A `fastexcel` error.
///
/// Contains a kind and a context. Use the `Display` trait to format the
/// error message with its context.
#[derive(Debug)]
pub struct FastExcelError {
    pub kind: FastExcelErrorKind,
    pub context: Vec<String>,
}

pub(crate) trait ErrorContext {
    fn with_context<S: ToString, F>(self, ctx_fn: F) -> Self
    where
        F: FnOnce() -> S;
}

impl FastExcelError {
    pub(crate) fn new(kind: FastExcelErrorKind) -> Self {
        Self {
            kind,
            context: vec![],
        }
    }
}

impl Display for FastExcelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{kind}", kind = self.kind)?;
        if !self.context.is_empty() {
            writeln!(f, "\nContext:")?;

            self.context
                .iter()
                .enumerate()
                .try_for_each(|(idx, ctx_value)| writeln!(f, "    {idx}: {ctx_value}"))?;
        }
        Ok(())
    }
}

impl Error for FastExcelError {}

impl ErrorContext for FastExcelError {
    fn with_context<S: ToString, F>(mut self, ctx_fn: F) -> Self
    where
        F: FnOnce() -> S,
    {
        self.context.push(ctx_fn().to_string());
        self
    }
}

impl From<FastExcelErrorKind> for FastExcelError {
    fn from(kind: FastExcelErrorKind) -> Self {
        FastExcelError::new(kind)
    }
}

impl From<XlsxError> for FastExcelError {
    fn from(err: XlsxError) -> Self {
        FastExcelErrorKind::CalamineError(calamine::Error::Xlsx(err)).into()
    }
}

pub type FastExcelResult<T> = Result<T, FastExcelError>;

impl<T> ErrorContext for FastExcelResult<T> {
    fn with_context<S: ToString, F>(self, ctx_fn: F) -> Self
    where
        F: FnOnce() -> S,
    {
        match self {
            Ok(_) => self,
            Err(e) => Err(e.with_context(ctx_fn)),
        }
    }
}

/// Contains Python versions of our custom errors
#[cfg(feature = "python")]
pub(crate) mod py_errors {
    use super::FastExcelErrorKind;
    use crate::error;
    use pyo3::{PyErr, PyResult, create_exception, exceptions::PyException};

    // Base fastexcel error
    create_exception!(
        _fastexcel,
        FastExcelError,
        PyException,
        "The base class for all fastexcel errors"
    );
    // Unsupported column type
    create_exception!(
        _fastexcel,
        UnsupportedColumnTypeCombinationError,
        FastExcelError,
        "Column contains an unsupported type combination"
    );
    // Cannot retrieve cell data
    create_exception!(
        _fastexcel,
        CannotRetrieveCellDataError,
        FastExcelError,
        "Data for a given cell cannot be retrieved"
    );
    // Calamine cell error
    create_exception!(
        _fastexcel,
        CalamineCellError,
        FastExcelError,
        "calamine returned an error regarding the content of the cell"
    );
    // Calamine error
    create_exception!(
        _fastexcel,
        CalamineError,
        FastExcelError,
        "Generic calamine error"
    );
    // Sheet not found
    create_exception!(
        _fastexcel,
        SheetNotFoundError,
        FastExcelError,
        "Sheet was not found"
    );
    // Sheet not found
    create_exception!(
        _fastexcel,
        ColumnNotFoundError,
        FastExcelError,
        "Column was not found"
    );
    // Arrow error
    create_exception!(
        _fastexcel,
        ArrowError,
        FastExcelError,
        "Generic arrow error"
    );
    // Invalid parameters
    create_exception!(
        _fastexcel,
        InvalidParametersError,
        FastExcelError,
        "Provided parameters are invalid"
    );
    // Invalid column
    create_exception!(
        _fastexcel,
        InvalidColumnError,
        FastExcelError,
        "Column is invalid"
    );
    // Internal error
    create_exception!(
        _fastexcel,
        InternalError,
        FastExcelError,
        "Internal fastexcel error"
    );

    impl From<error::FastExcelError> for PyErr {
        fn from(err: error::FastExcelError) -> Self {
            let message = err.to_string();
            match err.kind {
                FastExcelErrorKind::UnsupportedColumnTypeCombination(_) => {
                    UnsupportedColumnTypeCombinationError::new_err(message)
                }
                FastExcelErrorKind::CannotRetrieveCellData(_, _) => {
                    CannotRetrieveCellDataError::new_err(message)
                }
                FastExcelErrorKind::CalamineCellError(_) => CalamineCellError::new_err(message),
                FastExcelErrorKind::CalamineError(_) => CalamineError::new_err(message),
                FastExcelErrorKind::SheetNotFound(_) => SheetNotFoundError::new_err(message),
                FastExcelErrorKind::ColumnNotFound(_) => ColumnNotFoundError::new_err(message),
                FastExcelErrorKind::ArrowError(_) => ArrowError::new_err(message),
                FastExcelErrorKind::InvalidParameters(_) => {
                    InvalidParametersError::new_err(message)
                }
                FastExcelErrorKind::InvalidColumn(_) => InvalidColumnError::new_err(message),
                FastExcelErrorKind::Internal(_) => ArrowError::new_err(message),
            }
        }
    }

    pub(crate) trait IntoPyResult {
        type Inner;

        fn into_pyresult(self) -> PyResult<Self::Inner>;
    }

    impl<T> IntoPyResult for super::FastExcelResult<T> {
        type Inner = T;

        fn into_pyresult(self) -> PyResult<Self::Inner> {
            self.map_err(Into::into)
        }
    }
}
