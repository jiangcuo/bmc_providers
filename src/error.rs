use std::fmt;

#[derive(Debug)]
pub enum BmcError {
    Internal(String),
    BadRequest(String),
    Unsupported(String),
}

impl fmt::Display for BmcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BmcError::Internal(msg) => write!(f, "{}", msg),
            BmcError::BadRequest(msg) => write!(f, "{}", msg),
            BmcError::Unsupported(msg) => write!(f, "unsupported: {}", msg),
        }
    }
}

impl std::error::Error for BmcError {}

impl BmcError {
    pub fn internal(msg: impl Into<String>) -> Self {
        BmcError::Internal(msg.into())
    }

    pub fn bad_request(msg: impl Into<String>) -> Self {
        BmcError::BadRequest(msg.into())
    }
}

pub type BmcResult<T> = Result<T, BmcError>;
