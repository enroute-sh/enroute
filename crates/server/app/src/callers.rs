//! Who may call the contract, and on whose behalf.
//!
//! One header, naming the tenant, set by whatever authenticated the caller —
//! a proxy, a gateway, a mesh. Enroute checks nothing about it and cannot: the
//! deployment is what says who may reach this port, and this reads the answer.
//! **The contract listener must not be reachable except through that**, since
//! anything that can set the header is every tenant at once. Authenticating
//! only says *which* tenant; see [`crate::grpc`] for what a handler may reach
//! with it.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::HeaderName;
use tonic::Status;
use tonic::body::Body;
use tower::{Layer, Service};

use crate::tenancy::{Tenant, Tenants};

/// Refuses any call carrying no token this Enroute knows, and hands the ones
/// it does know the tenant they belong to.
#[derive(Debug, Clone)]
pub(crate) struct Authenticate {
    tenants: Arc<Tenants>,
    /// What the deployment's proxy names the tenant in.
    header: HeaderName,
}

impl Authenticate {
    /// Read the tenant of every contract call out of `header`.
    #[must_use]
    pub(crate) fn new(tenants: Arc<Tenants>, header: HeaderName) -> Self {
        Self { tenants, header }
    }
}

impl<S> Layer<S> for Authenticate {
    type Service = Authenticated<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Authenticated {
            inner,
            tenants: Arc::clone(&self.tenants),
            header: self.header.clone(),
        }
    }
}

/// One service behind [`Authenticate`].
#[derive(Debug, Clone)]
pub struct Authenticated<S> {
    inner: S,
    tenants: Arc<Tenants>,
    header: HeaderName,
}

impl<S> Service<http::Request<Body>> for Authenticated<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        // `self` is the pooled service; the clone is the one that is actually
        // ready. Swapping keeps the readiness this call already reserved.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        // Resolved here rather than inside the future: the tenants are in
        // memory, so this reaches nothing and needs neither to be awaited nor
        // to clone what it reads.
        let Some(tenant) = self.resolve(&request) else {
            return Box::pin(async move { Ok(refused().into_http()) });
        };
        // Which caller, at debug: enough to answer "who is hammering this"
        // without a line per request in a normal deployment.
        tracing::debug!(tenant = %tenant.id, "contract call");
        // Read back out by `crate::grpc::Api::opened`, which is the only
        // way a handler gets a repository id it may use.
        request.extensions_mut().insert(tenant);
        Box::pin(async move { inner.call(request).await })
    }
}

/// Delegated rather than invented: this layer decides who may call, never
/// what they reached.
///
/// Required so `tonic`'s router, which dispatches on the wrapped service's
/// name, can route through this layer.
impl<S: tonic::server::NamedService> tonic::server::NamedService for Authenticated<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> Authenticated<S> {
    /// The tenant this call is for, if it named one this serves.
    ///
    /// Every way of failing answers alike, and says why only to a log: a caller
    /// learning which ids exist learns the shape of every other customer.
    fn resolve(&self, request: &http::Request<Body>) -> Option<Tenant> {
        match named(request, &self.header) {
            Err(error) => {
                tracing::debug!(%error, "a contract call named no tenant");
                None
            }
            Ok(named) => {
                let found = self.tenants.by_id(&named);
                if found.is_none() {
                    tracing::debug!(named, "a contract call named no tenant this serves");
                }
                found
            }
        }
    }
}

/// The one refusal, spelled once so the paths to it cannot drift.
fn refused() -> Status {
    Status::unauthenticated("no tenant was named")
}

/// The tenant this request is for, from the header the deployment names.
///
/// More than one is refused rather than read, since a proxy that adds the
/// header without stripping what arrived leaves the caller's beside its own.
fn named(request: &http::Request<Body>, header: &HeaderName) -> Result<String, &'static str> {
    let mut named = request.headers().get_all(header).into_iter();
    let (Some(one), None) = (named.next(), named.next()) else {
        return Err("no tenant header, or more than one");
    };
    let named = one
        .to_str()
        .map_err(|_bytes| "a tenant header that is not text")?;
    if named.is_empty() {
        return Err("an empty tenant header");
    }
    Ok(named.to_string())
}

/// The tenant a call authenticated as.
///
/// Never `Ok` for an unauthenticated call: the layer rejects those before a
/// handler is reached, so an absent tenant is a wiring bug, not a caller's.
///
/// # Errors
///
/// Returns `Internal` if no tenant was attached to the request.
pub(crate) fn tenant_of<T>(request: &tonic::Request<T>) -> Result<Tenant, Status> {
    request
        .extensions()
        .get::<Tenant>()
        .cloned()
        .ok_or_else(|| {
            tracing::error!("a contract handler ran with no tenant attached");
            Status::internal("internal error")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "x-enroute-tenant";

    fn header() -> HeaderName {
        HeaderName::from_static(HEADER)
    }

    fn naming(values: &[&str]) -> http::Request<Body> {
        let mut request = http::Request::new(Body::empty());
        for value in values {
            request
                .headers_mut()
                .append(header(), value.parse().expect("a header value"));
        }
        request
    }

    #[test]
    fn the_tenant_is_read_out_of_the_header() {
        assert_eq!(named(&naming(&["acme"]), &header()).unwrap(), "acme");
    }

    #[test]
    fn a_request_naming_nobody_is_refused() {
        named(&naming(&[]), &header()).expect_err("no tenant header");
        named(&naming(&[""]), &header()).expect_err("an empty tenant header");
    }

    /// The one that matters.
    ///
    /// A proxy adding the header without stripping what arrived leaves two,
    /// and either of them could be the caller's.
    #[test]
    fn a_request_naming_two_tenants_is_refused() {
        named(&naming(&["acme", "other"]), &header()).expect_err("two tenant headers");
        // Even agreeing with itself, since what agreed is not knowable here.
        named(&naming(&["acme", "acme"]), &header()).expect_err("two tenant headers");
    }

    /// Telling a caller that the tenant they named is not one this serves
    /// tells them which ids exist.
    #[test]
    fn every_refusal_reads_the_same() {
        let one = refused();
        let two = refused();
        assert_eq!(one.code(), tonic::Code::Unauthenticated);
        assert_eq!(one.message(), two.message());
    }

    #[test]
    fn a_handler_with_no_tenant_is_an_internal_error() {
        let status = tenant_of(&tonic::Request::new(())).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
    }
}
