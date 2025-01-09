//! Mapping from a Connectors request or response to GraphQL

use std::collections::HashMap;
use std::fmt::Display;

use apollo_federation::sources::connect::ApplyToError;
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

/// A mapping problem
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub(crate) struct Problem {
    pub(crate) level: Level,
    pub(crate) message: String,
    pub(crate) path: String,
    pub(crate) count: usize,
}

/// The level of a mapping problem
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub(crate) enum Level {
    Error = 0,
    Warn = 1,
    Info = 2,
    None = 3,
}

impl Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Level::Error => write!(f, "error"),
            Level::Warn => write!(f, "warn"),
            Level::Info => write!(f, "info"),
            Level::None => write!(f, "none"),
        }
    }
}

/// Aggregate a list of [`ApplyToError`] into [mapping problems](Problem)
pub(crate) fn aggregate_apply_to_errors(errors: &[ApplyToError]) -> Vec<Problem> {
    errors
        .iter()
        .fold(
            HashMap::default(),
            |mut acc: HashMap<(&str, String), usize>, err| {
                let path = err
                    .path()
                    .iter()
                    .map(|p| match p.as_u64() {
                        Some(_) => "@", // ignore array indices for grouping
                        None => p.as_str().unwrap_or_default(),
                    })
                    .join(".");

                acc.entry((err.message(), path))
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                acc
            },
        )
        .iter()
        .map(|(key, &count)| Problem {
            level: Level::Error, // TODO: real value when added to `ApplyToError`
            message: key.0.to_string(),
            path: key.1.clone(),
            count,
        })
        .collect()
}
