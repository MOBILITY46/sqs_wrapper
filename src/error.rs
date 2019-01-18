use std::{error::Error as StdError, fmt};

#[derive(Debug)]
pub enum Error {
    Sqs(String),
    Json(serde_json::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Error::Sqs(ref msg) => write!(f, "SQS error: {}", msg),
            Error::Json(ref err) => write!(f, "Json Error: {}", err),
        }
    }
}

impl StdError for Error {
    fn cause(&self) -> Option<&dyn StdError> {
        match *self {
            Error::Json(ref e) => Some(e),
            _ => None,
        }
    }

    fn description(&self) -> &'static str {
        match *self {
            Error::Sqs(_) => "Sqs library returned an error",
            Error::Json(_) => "Serde Json returned an error",
        }
    }
}

impl From<rusoto_core::CredentialsError> for Error {
    fn from(error: rusoto_core::CredentialsError) -> Self {
        Error::Sqs(format!("CredentialsError: {}", error))
    }
}

impl From<rusoto_core::HttpDispatchError> for Error {
    fn from(error: rusoto_core::HttpDispatchError) -> Self {
        Error::Sqs(format!("Http Error: {}", error))
    }
}

impl From<rusoto_sqs::ReceiveMessageError> for Error {
    fn from(error: rusoto_sqs::ReceiveMessageError) -> Self {
        Error::Sqs(format!("Error receiving messages: {}", error))
    }
}
