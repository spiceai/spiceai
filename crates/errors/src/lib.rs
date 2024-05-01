use core::fmt;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub enum Level {
    Warn,
    Error,
    Panic,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpiceError {
    pub level: Level,
    pub message: String,
    pub friendly_message: String,
}

impl SpiceError {
    pub fn new<E: Into<SpiceError>>(source: E, friendly_message: String) -> Self
    where
        E: Debug,
    {
        SpiceError {
            level: Level::Error,
            message: format!("{:?}", source),
            friendly_message,
        }
    }

    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    pub fn from<E: Into<SpiceError>>(error: E) -> SpiceError
    where
        E: Debug,
    {
        let spice_error: SpiceError = error.into();
        spice_error
    }
}

impl fmt::Display for SpiceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("{}", self.message))
    }
}

impl std::error::Error for SpiceError {}

pub fn trace<E: Into<SpiceError>>(error: E) -> SpiceError
where
    E: Debug,
{
    let spice_error = SpiceError::from(error);

    tracing::debug!("Internal Error: {}", spice_error.message);

    match spice_error.level {
        Level::Warn => tracing::warn!("{}", spice_error.friendly_message),
        Level::Error => tracing::error!("{}", spice_error.friendly_message),
        Level::Panic => panic!("{}", spice_error.friendly_message),
    }

    spice_error
}
