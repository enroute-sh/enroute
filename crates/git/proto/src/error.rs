/// Errors that can occur while handling a git protocol request.
///
/// Nothing here says "unauthorized" or "forbidden": that is decided above
/// this layer, by a `enroute_git_http::Authorizer`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The request was malformed.
    #[error("bad request: {0}")]
    BadRequest(String),
    /// A lower-level failure (object codec, store, I/O) with no protocol-specific meaning.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<crate::pktline::Malformed> for Error {
    fn from(e: crate::pktline::Malformed) -> Self {
        Error::BadRequest(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Internal(e.into())
    }
}

impl From<enroute_git_core::Error> for Error {
    fn from(err: enroute_git_core::Error) -> Self {
        match err {
            enroute_git_core::Error::Internal(e) => Error::Internal(e),
            enroute_git_core::Error::Invalid(msg) => Error::BadRequest(msg),
            // What the client asked for, not a fault of ours: a `want` this
            // repository cannot produce is answered rather than logged.
            enroute_git_core::Error::Missing(oid) => Error::BadRequest(format!("no object {oid}")),
        }
    }
}
