//! The contract, described by the server that serves it.
//!
//! What a client asks when it would rather read the schema off a running
//! Enroute than be handed a `.proto` out of band.

use prost::Message as _;
use tokio_stream::StreamExt as _;
use tonic_reflection::pb::v1::ServerReflectionRequest;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;

use crate::support::{make_isolated_state, spawn_contract_without_hooks};

/// One reflection call against a contract, answered.
///
/// No tenant header anywhere in here, unlike every other client in these
/// tests: the four services are guarded and this is deliberately not.
async fn ask(request: MessageRequest) -> MessageResponse {
    let state = make_isolated_state().await;
    let addr = spawn_contract_without_hooks(state).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("a usable contract URL")
        .connect()
        .await
        .expect("connecting to reflection");
    let mut client = ServerReflectionClient::new(channel);

    let asked = tokio_stream::iter(vec![ServerReflectionRequest {
        host: String::new(),
        message_request: Some(request),
    }]);

    client
        .server_reflection_info(asked)
        .await
        .expect("the reflection call")
        .into_inner()
        .next()
        .await
        .expect("an answer")
        .expect("an answer that is not a status")
        .message_response
        .expect("an answer with a body")
}

#[tokio::test]
async fn it_lists_every_service_the_contract_holds() {
    let MessageResponse::ListServicesResponse(listed) =
        ask(MessageRequest::ListServices(String::new())).await
    else {
        panic!("asked to list services and got something else");
    };

    let names: Vec<&str> = listed.service.iter().map(|s| s.name.as_str()).collect();
    for wanted in [
        "enroute.api.v1alpha1.RepositoryService",
        "enroute.api.v1alpha1.RefService",
        "enroute.api.v1alpha1.ObjectService",
        "enroute.api.v1alpha1.SyncService",
    ] {
        assert!(
            names.contains(&wanted),
            "{wanted} was not listed: {names:?}"
        );
    }

    // The listing is the whole of what a client discovers on its own, and the
    // hook contract is not in it: an integrator is told about that file, never
    // walked to it. Everything else listed is reflection describing itself,
    // which is what the docs say a `list` comes back with.
    let rest: Vec<&&str> = names
        .iter()
        .filter(|name| !name.starts_with("enroute.api.v1alpha1."))
        .collect();
    assert!(
        rest.iter().all(|name| name.starts_with("grpc.reflection.")),
        "the hook package declares no service and must not be listed: {names:?}"
    );
}

/// Either hook message, asked for by name, hands back the whole file.
///
/// Reflection answers with the file a symbol sits in, so one question about
/// `HookRequest` describes `HookResponse` alongside it.
#[tokio::test]
async fn it_describes_the_hook_contract_too() {
    for symbol in [
        "enroute.hook.v1alpha1.HookRequest",
        "enroute.hook.v1alpha1.HookResponse",
    ] {
        let MessageResponse::FileDescriptorResponse(files) =
            ask(MessageRequest::FileContainingSymbol(symbol.to_string())).await
        else {
            panic!("asked for {symbol} and got something else");
        };

        let described: Vec<String> = files
            .file_descriptor_proto
            .iter()
            .flat_map(|bytes| {
                prost_types::FileDescriptorProto::decode(&bytes[..])
                    .expect("a file descriptor")
                    .message_type
                    .into_iter()
                    .filter_map(|message| message.name)
            })
            .collect();

        for wanted in ["HookRequest", "HookResponse"] {
            assert!(
                described.contains(&wanted.to_string()),
                "asking for {symbol} did not describe {wanted}: {described:?}"
            );
        }
    }
}
