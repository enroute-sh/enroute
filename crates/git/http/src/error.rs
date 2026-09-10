use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::Challenge;

/// Every way a git route can fail, mapped to an HTTP status.
///
/// A 401 is a variant, not a derived status — it must say how to try again,
/// so naming it needs a [`Challenge`] and a challenge-less 401 can't compile.
#[derive(Debug, thiserror::Error)]
pub(crate) enum HttpError {
    #[error("unauthorized")]
    Unauthorized(Challenge),
    #[error("forbidden")]
    Forbidden,
    #[error("repository not found")]
    NotFound,
    #[error("Git-Protocol: version=2 required")]
    ProtocolVersionRequired,
    #[error(transparent)]
    Protocol(#[from] enroute_git_proto::Error),
}

impl From<anyhow::Error> for HttpError {
    fn from(e: anyhow::Error) -> Self {
        enroute_git_proto::Error::from(e).into()
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::ProtocolVersionRequired
            | Self::Protocol(enroute_git_proto::Error::BadRequest(_)) => StatusCode::BAD_REQUEST,
            Self::Protocol(enroute_git_proto::Error::Internal(_)) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %self, "internal error handling git request");
        }
        let body = self.to_string();
        match self {
            // Git's own credential handling (netrc, credential helpers, a
            // prompted retry) only kicks in off this header on a 401, and the
            // challenge carries its own body: how to try again.
            Self::Unauthorized(challenge) => (
                status,
                [(header::WWW_AUTHENTICATE, challenge.www_authenticate)],
                challenge.help,
            )
                .into_response(),
            _ => (status, body).into_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Git only surfaces an error body (as `remote:` lines) when it's
    /// `text/plain`, so the content type is load-bearing, not incidental.
    #[test]
    fn unauthorized_carries_its_challenge_as_plain_text() {
        let challenge = Challenge::basic("test-realm", "try a token.\n");
        let resp = HttpError::Unauthorized(challenge).into_response();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/plain; charset=utf-8"),
        );
        assert_eq!(
            resp.headers()
                .get(header::WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok()),
            Some("Basic realm=\"test-realm\""),
        );
    }
}
