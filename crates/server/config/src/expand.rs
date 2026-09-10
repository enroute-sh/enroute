//! `${VAR}` in a string, replaced by what the environment says.
//!
//! Every value the file writes as a string goes through here — a URI, a
//! header name, an address, a function's name — so the rule is one a reader
//! can hold: anything in quotes may interpolate, and a number or a boolean
//! may not. It happens as each value is deserialized rather than over the
//! file's text, because a substitution made before parsing can close a quote
//! or open a table, and what is being substituted is the part a deployment
//! did not write. Unset is an error, not an empty string: `s3:///objects` is
//! a bucket nobody meant and would be found much later than here.

use anyhow::{Result, bail};

/// Expand every `${VAR}` in `raw`.
///
/// `$$` is a literal `$`, and a `$` before anything else is refused: `$HOME`
/// is a mistake far more often than a value.
///
/// # Errors
///
/// Returns an error if a name is unset, malformed, or unterminated, or if a
/// `$` is not part of either form.
pub(crate) fn expand(raw: &str) -> Result<String> {
    expand_from(raw, |name| std::env::var(name).ok())
}

/// The same, against whatever `lookup` says a name is.
///
/// The environment is the one caller and a test is the other: reading it is a
/// process-wide thing to do, and writing it is worse.
fn expand_from(raw: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<String> {
    // The overwhelming case, and it allocates once instead of growing.
    if !raw.contains('$') {
        return Ok(raw.to_string());
    }

    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some((before, after)) = rest.split_once('$') {
        out.push_str(before);

        if let Some(tail) = after.strip_prefix('$') {
            out.push('$');
            rest = tail;
        } else if let Some(opened) = after.strip_prefix('{') {
            let Some((name, tail)) = opened.split_once('}') else {
                bail!("{raw} opens a ${{ that is never closed");
            };
            check(name, raw)?;
            let Some(value) = lookup(name) else {
                // The name and never the value: what this expands is usually
                // the reason a value was not written in the file.
                bail!("{name} names no value in this environment");
            };
            out.push_str(&value);
            rest = tail;
        } else {
            bail!("{raw} has a $ that begins neither ${{NAME}} nor a literal $$");
        }
    }
    out.push_str(rest);
    Ok(out)
}

/// # Errors
///
/// Returns an error if `name` is not one an environment could hold.
fn check(name: &str, raw: &str) -> Result<()> {
    if name.is_empty() {
        bail!("{raw} names no variable between ${{ and }}");
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        || name.starts_with(|c: char| c.is_ascii_digit())
    {
        bail!("{name} is not a variable name: letters, digits and _, not starting with a digit");
    }
    Ok(())
}

/// [`expand`], as a `serde` error rather than an [`anyhow::Error`].
///
/// `{:#}` for the whole chain, in one place: the outer message alone names a
/// string without saying which part of it could not be expanded.
fn expanded<E: serde::de::Error>(raw: &str) -> Result<String, E> {
    expand(raw).map_err(|error| E::custom(format!("{error:#}")))
}

/// A `String` field, expanded.
pub(super) mod string {
    use serde::{Deserialize as _, Deserializer};

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<String, D::Error> {
        super::expanded(&String::deserialize(deserializer)?)
    }
}

/// An optional `String` field, expanded when it is there.
#[cfg(feature = "file")]
pub(super) mod optional {
    use serde::{Deserialize as _, Deserializer};

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<String>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|raw| super::expanded(&raw))
            .transpose()
    }
}

/// Anything that parses from a string, expanded before it is parsed.
///
/// Where the schema asks for `${VAR}`, rather than the type deciding: the URI
/// types are also read from the ingest call, which expands nothing.
pub(super) mod parsed {
    use std::fmt::Display;
    use std::str::FromStr;

    use serde::{Deserializer, de};

    pub(crate) fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: FromStr,
        T::Err: Display,
    {
        super::string::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An environment holding what these tests name, and nothing else.
    fn env(raw: &str) -> Result<String> {
        expand_from(raw, |name| match name {
            "REGION" => Some("eu-central-1".to_string()),
            "ZONE" => Some("eu".to_string()),
            "HOST" => Some("127.0.0.1".to_string()),
            "PORT" => Some("50051".to_string()),
            "SECRETISH" => Some("hunter2".to_string()),
            _ => None,
        })
    }

    #[test]
    fn a_string_with_no_dollar_is_itself() {
        assert_eq!(env("s3://objects/prefix").unwrap(), "s3://objects/prefix");
    }

    #[test]
    fn a_variable_is_what_the_environment_says() {
        assert_eq!(env("${REGION}").unwrap(), "eu-central-1");
    }

    /// The reason for interpolating rather than replacing a whole value: the
    /// variable is part of the string and not the string.
    #[test]
    fn a_variable_inside_a_longer_string_is_expanded_in_place() {
        assert_eq!(
            env("s3://enroute-${ZONE}/objects").unwrap(),
            "s3://enroute-eu/objects"
        );
    }

    #[test]
    fn two_variables_in_one_string_are_both_expanded() {
        assert_eq!(env("${HOST}:${PORT}").unwrap(), "127.0.0.1:50051");
    }

    /// Unset is refused rather than empty, because what an empty one builds
    /// is `s3:///objects` — a bucket nobody named.
    #[test]
    fn a_name_the_environment_does_not_have_is_refused() {
        let error = env("${NOWHERE}").unwrap_err().to_string();
        assert!(error.contains("NOWHERE"), "{error}");
    }

    #[test]
    fn a_literal_dollar_is_written_twice() {
        assert_eq!(env("pa$$word").unwrap(), "pa$word");
        assert_eq!(env("$${REGION}").unwrap(), "${REGION}");
    }

    /// `$HOME` is a mistake far more often than a value, and a lenient reading
    /// would leave it in the config looking like it had worked.
    #[test]
    fn a_bare_dollar_name_is_refused_rather_than_kept() {
        let error = env("$HOME/objects").unwrap_err().to_string();
        assert!(error.contains("literal $$"), "{error}");
    }

    #[test]
    fn an_unclosed_variable_is_refused() {
        let error = env("s3://${OPEN").unwrap_err().to_string();
        assert!(error.contains("never closed"), "{error}");
    }

    #[test]
    fn a_name_that_is_not_one_is_refused() {
        for raw in ["${}", "${a-b}", "${1UP}", "${a b}"] {
            env(raw).unwrap_err();
        }
    }

    /// The value is what a deployment kept out of the file, so a failure that
    /// printed it would put it back.
    #[test]
    fn a_failure_names_the_variable_and_never_its_value() {
        let error = env("${SECRETISH}x${NOWHERE}").unwrap_err().to_string();
        assert!(error.contains("NOWHERE"), "{error}");
        assert!(!error.contains("hunter2"), "{error}");
    }

    /// What the environment holds is not read as more `${}`, so a value
    /// cannot smuggle in another lookup.
    #[test]
    fn an_expanded_value_is_not_expanded_again() {
        let once = expand_from("${OUTER}", |name| match name {
            "OUTER" => Some("${INNER}".to_string()),
            _ => panic!("{name} was looked up, so a value was expanded twice"),
        });
        assert_eq!(once.unwrap(), "${INNER}");
    }
}
