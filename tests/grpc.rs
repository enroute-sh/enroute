//! The `Ledger` gRPC service between a ledger node (the server) and a
//! compute node (the client), with a stand-in ledger.

#![expect(
    clippy::as_conversions,
    clippy::indexing_slicing,
    reason = "a test fails by panicking, on values it chose itself"
)]

use std::sync::Arc;

use tonic::{Request, Response, Status};

use enroute_compute::testing::client;
use enroute_shared::proto::{
    ActivateRequest, ActivateResponse, DeleteRequest, DeleteResponse, DescribeRequest,
    DescribeResponse, ListRequest, ListResponse, PendingRequest, PendingResponse,
    ReadBarrierRequest, ReadBarrierResponse, WatchRequest, ledger_server,
};
use enroute_shared::roles::WatchStream;
use enroute_shared::state::{Command, Commit, LayerMeta, Outcome};

/// Answers from the request, so the test sees what crossed the wire.
struct Echo;

#[tonic::async_trait]
impl ledger_server::Ledger for Echo {
    async fn activate(
        &self,
        req: Request<ActivateRequest>,
    ) -> Result<Response<ActivateResponse>, Status> {
        let r = req.into_inner();
        if r.repo == "missing" {
            return Err(enroute_shared::roles::status(anyhow::anyhow!(
                "no such repository: {}",
                r.repo
            )));
        }
        Ok(Response::new(ActivateResponse { exists: r.create }))
    }

    async fn propose(&self, req: Request<Command>) -> Result<Response<Outcome>, Status> {
        let Some(enroute_shared::state::command::Kind::Commit(c)) = req.into_inner().kind else {
            return Err(Status::invalid_argument("not a commit"));
        };
        Ok(Response::new(Outcome {
            repo_version: c.add_layers[0].data.as_ref().map_or(0, |d| d.len() as u64),
            ..Default::default()
        }))
    }

    async fn read_barrier(
        &self,
        _req: Request<ReadBarrierRequest>,
    ) -> Result<Response<ReadBarrierResponse>, Status> {
        Ok(Response::new(ReadBarrierResponse { version: 42 }))
    }

    async fn pending(
        &self,
        _req: Request<PendingRequest>,
    ) -> Result<Response<PendingResponse>, Status> {
        Ok(Response::new(PendingResponse::default()))
    }

    async fn delete(
        &self,
        _req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        Err(Status::unimplemented("delete"))
    }

    async fn describe(
        &self,
        _req: Request<DescribeRequest>,
    ) -> Result<Response<DescribeResponse>, Status> {
        Err(Status::unimplemented("describe"))
    }

    async fn list(&self, _req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        Err(Status::unimplemented("list"))
    }

    type WatchStream = WatchStream;

    async fn watch(
        &self,
        _req: Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<Response<WatchStream>, Status> {
        Err(Status::unimplemented("watch"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ledger_over_grpc() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = ledger_server::LedgerServer::from_arc(Arc::new(Echo))
        .max_decoding_message_size(enroute_shared::proto::MAX_MESSAGE);
    let router = tonic::service::Routes::new(server).into_axum_router();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut c = client(&format!("http://{addr}")).unwrap();

    let activate = |repo: &str, create| ActivateRequest {
        repo: repo.into(),
        create,
        ..Default::default()
    };
    assert!(
        c.activate(activate("r", true))
            .await
            .unwrap()
            .into_inner()
            .exists
    );
    assert!(
        !c.activate(activate("r", false))
            .await
            .unwrap()
            .into_inner()
            .exists
    );
    let err =
        enroute_shared::roles::error(c.activate(activate("missing", true)).await.unwrap_err())
            .to_string();
    assert!(err.contains("no such repository: missing"), "{err}");
    // An inline layer far above gRPC's default 4 MiB message limit.
    let data = bytes::Bytes::from(vec![7u8; 6 << 20]);
    let layer = LayerMeta {
        id: "l".into(),
        data: Some(data),
        ..Default::default()
    };
    let out = c
        .propose(Command::commit(
            "r",
            Commit {
                add_layers: vec![layer],
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    assert_eq!(out.into_inner().repo_version, 6 << 20);
    assert_eq!(
        c.read_barrier(ReadBarrierRequest { repo: "r".into() })
            .await
            .unwrap()
            .into_inner()
            .version,
        42
    );
}
