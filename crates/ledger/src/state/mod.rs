//! The repositories' state machines: the rules that commands follow
//! ([`machine`]), the node's state store that holds every state on disk
//! ([`store`]), and how a repository's state maps onto it ([`stored`]).

pub(crate) mod machine;
pub(crate) mod store;
pub(crate) mod stored;
