use core::fmt;
use std::{error::Error, fmt::Debug, iter};

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

    pub fn get_message(&self) -> String {
        let mut last_error = self;

        // let test: SpiceError = self.type_id();

        // for e in iter::successors(self.source(), |&e| e.source()) {
        //     if let Ok(error) = e.try_into() {
        //         let spice_error: SpiceError = error;
        //         if !spice_error.friendly_message.is_empty() {
        //             last_error = Some(spice_error);
        //         }
        //     }
        // }

        return last_error.friendly_message.to_string();
    }
}

impl fmt::Display for SpiceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("{}", self.message))
    }
}

// impl TryFrom<&dyn std::error::Error> for SpiceError {
//     type Error = SpiceError;

//     fn try_from(error: &dyn std::error::Error) -> Result<Self, Self::Error> {
//         let spice_error = SpiceError {
//             level: Level::Error,
//             message: format!("{:?}", error),
//             friendly_message: "".to_string(),
//         };

//         Ok(spice_error)
//     }
// }

impl std::error::Error for SpiceError {}

pub fn trace<E: Into<SpiceError>>(error: E)
where
    E: Debug + std::error::Error,
{
    let spice_error = SpiceError::from(error);
    tracing::debug!("Internal Error: {}", spice_error.message);

    match spice_error.level {
        Level::Warn => tracing::warn!("{}", spice_error.friendly_message),
        Level::Error => tracing::error!("{}", spice_error.friendly_message),
        Level::Panic => panic!("{}", spice_error.friendly_message),
    }

    // let mut last_error: Option<SpiceError> = None;

    // for e in iter::successors(error.source(), |&e| e.source()) {
    //     if let Ok(error) = e.try_into() {
    //         let spice_error: SpiceError = error;
    //         if !spice_error.friendly_message.is_empty() {
    //             last_error = Some(spice_error);
    //         }
    //     }
    // }

    // if let Some(spice_error) = last_error {
    //     tracing::debug!("Internal Error: {}", spice_error.message);

    //     match spice_error.level {
    //         Level::Warn => tracing::warn!("{}", spice_error.friendly_message),
    //         Level::Error => tracing::error!("{}", spice_error.friendly_message),
    //         Level::Panic => panic!("{}", spice_error.friendly_message),
    //     }
    // }
}
