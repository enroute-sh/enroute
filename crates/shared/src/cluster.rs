//! The ledger's members as the object store has them, at [`MEMBERS`]: how
//! nodes of either role find the ledger. Only ledger nodes write it (the
//! root log's leader, when the members change), so what it says is the
//! ledger's own word, if maybe out of date; compute nodes only read it.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::store::Objects;

/// Where the members are.
pub const MEMBERS: &str = "cluster/members";

/// The object at [`MEMBERS`].
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Cluster {
    /// The node that started the root log.
    pub bootstrap: u64,
    /// The members, as the root log's leader last wrote them.
    #[serde(default)]
    pub members: Vec<Node>,
}

/// A ledger node.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// Its id (its log store's).
    pub id: u64,
    /// Its internal URL.
    pub url: String,
    /// The public key that checks the tokens it signs, as the ledger
    /// encodes it.
    pub key: String,
    /// Its failure domain, such as an availability zone (empty: none
    /// named).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub zone: String,
}

/// The object at [`MEMBERS`]; None before a cluster starts.
///
/// # Errors
///
/// Returns an error if the store fails or the object is malformed.
pub async fn read(objects: &Objects) -> Result<Option<Cluster>> {
    let Some(data) = objects.get_object(MEMBERS).await? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_slice(&data)?))
}
