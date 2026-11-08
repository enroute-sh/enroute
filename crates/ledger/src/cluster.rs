//! The ledger's membership: who the members are, where they are, and the
//! keys that check their tokens, all in the root log (`consensus::root`);
//! and how a node finds the ledger and joins it.
//!
//! The object store is only the rendezvous, under `cluster/`, read when a
//! node starts and written when the membership changes, never polled:
//!
//! - `cluster/members`: created once by a new cluster's first node, which
//!   starts the root log ([`bootstraps`]); the root log's leader rewrites
//!   it with the members (id, address, key) when they change ([`publish`]).
//!   A starting ledger node reads it to find a member to join through, and
//!   a compute node to find the ledger. A stale list is harmless: any
//!   member forwards a join to the root log.
//! - `cluster/nodes/<id>`: a node's address and token key, written by the
//!   node when it starts ([`announce`]). It is what a join asks for, and
//!   since only ledger nodes may write `cluster/`, it is the join's proof.
//!
//! Liveness is the engine's own pings (`consensus::control`), and a
//! compute's is the ledger's to note from its `Pending` calls.

use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{Context as _, Result};
use tonic::{Request, Response, Status};

use enroute_shared::cluster::{Cluster, MEMBERS, Node};
use enroute_shared::store::Objects;

use crate::consensus::Engine;
use crate::logdb::LogDb;
use crate::proto::{
    JoinRequest, JoinResponse, Member, MembersRequest, MembersResponse, RemoveRequest,
    RemoveResponse, cluster_client::ClusterClient, cluster_server, root_entry,
};
use crate::token;

/// The cluster's rendezvous (see the module docs).
const CLUSTER: &str = MEMBERS;
/// Where ledger nodes announce themselves.
const NODES: &str = "cluster/nodes/";
/// How often a node that is not a member at its address yet asks to be.
const JOIN_EVERY: Duration = Duration::from_secs(1);
/// How long a join waits for one member, so a frozen one does not keep the
/// others from being asked: past the root log's own wait to admit.
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);

fn node_path(id: u64) -> String {
    format!("{NODES}{id:016x}")
}

impl From<Node> for Member {
    fn from(n: Node) -> Member {
        Member {
            id: n.id,
            url: n.url,
            key: n.key,
            zone: n.zone,
        }
    }
}

impl From<Member> for Node {
    fn from(m: Member) -> Node {
        Node {
            id: m.id,
            url: m.url,
            key: m.key,
            zone: m.zone,
        }
    }
}

/// Whether node `id` starts the root log, having none yet: a standalone
/// node does; in a cluster, the node that creates [`CLUSTER`] (or did, and
/// stopped before it wrote its root). A node with a root makes sure the
/// object exists, so a standalone node that becomes a cluster's first
/// member is the only node with a root.
pub(crate) async fn bootstraps(
    db: &LogDb,
    objects: &Objects,
    id: u64,
    cluster: bool,
) -> Result<bool> {
    let me = serde_json::to_vec(&Cluster {
        bootstrap: id,
        members: Vec::new(),
    })?;
    if crate::consensus::root::exists(db) {
        if cluster {
            objects.put_if(CLUSTER, me.into(), None).await?;
        }
        return Ok(false);
    }
    if !cluster {
        return Ok(true);
    }
    if objects.put_if(CLUSTER, me.into(), None).await?.is_some() {
        return Ok(true);
    }
    Ok(read_cluster(objects).await?.bootstrap == id)
}

async fn read_cluster(objects: &Objects) -> Result<Cluster> {
    enroute_shared::cluster::read(objects)
        .await?
        .context("the cluster object vanished")
}

/// Announce this node as `me`: its address and token key.
///
/// # Errors
///
/// Returns an error if the store fails.
pub(crate) async fn announce(objects: &Objects, me: &Member) -> Result<()> {
    let entry = Node::from(me.clone());
    objects
        .put_object(&node_path(me.id), serde_json::to_vec(&entry)?.into())
        .await
}

/// Node `id` as it announced itself.
async fn announced(objects: &Objects, id: u64) -> Result<Option<Member>> {
    let Some(data) = objects.get_object(&node_path(id)).await? else {
        return Ok(None);
    };
    let e: Node = serde_json::from_slice(&data)?;
    Ok((e.id == id).then(|| e.into()))
}

/// Rewrite [`CLUSTER`] with the members in `engine`'s directory, if they
/// changed since `last` (which it updates).
pub(crate) async fn publish(engine: &Engine, objects: &Objects, last: &mut Vec<Member>) {
    let peers = engine.peers();
    if peers.is_empty() || peers == *last {
        return;
    }
    let written = async {
        let mut c = read_cluster(objects).await?;
        c.members = peers.iter().cloned().map(Node::from).collect();
        objects
            .put_object(CLUSTER, serde_json::to_vec(&c)?.into())
            .await
    };
    match written.await {
        Ok(()) => *last = peers,
        Err(e) => tracing::warn!("writing the members: {e:#}"),
    }
}

/// The members [`CLUSTER`] names. Only ledger nodes write `cluster/`, so
/// what it says is the ledger's own word, if maybe out of date.
///
/// # Errors
///
/// Returns an error if the store fails. None before a cluster starts.
pub async fn named(objects: &Objects) -> Result<Vec<Member>> {
    let c = enroute_shared::cluster::read(objects)
        .await?
        .unwrap_or_default();
    Ok(c.members.into_iter().map(Member::from).collect())
}

/// Keep asking to be member `me` (see `Join`) until `engine`'s directory
/// has it, through itself if it is a member already, else through the
/// members [`CLUSTER`] names (which `engine` learns of meanwhile, to talk
/// to them before it has the directory). It stops once `engine` is dropped.
pub(crate) async fn join(engine: Weak<Engine>, objects: Arc<Objects>, me: Member) {
    let mut failed = 0u32;
    loop {
        let Some(e) = engine.upgrade() else { return };
        if e.peer(me.id).as_ref() == Some(&me) {
            return;
        }
        let asked = if e.members().0.contains(&me.id) {
            admit(&e, &objects, me.id)
                .await
                .map_err(anyhow::Error::from)
        } else {
            join_through(&e, &objects, &me).await
        };
        drop(e);
        if let Err(err) = asked {
            failed += 1;
            // Every ten seconds: a cluster that is starting takes a while.
            if failed % 10 == 1 {
                tracing::warn!(node = me.id, "joining the ledger: {err:#}");
            }
        }
        tokio::time::sleep(JOIN_EVERY).await;
    }
}

/// Ask the members [`CLUSTER`] names, in turn, to admit `me`.
async fn join_through(engine: &Engine, objects: &Objects, me: &Member) -> Result<()> {
    let members = named(objects).await?;
    engine.introduce(&members);
    let mut last = anyhow::anyhow!("{CLUSTER} names no member yet");
    for m in members.iter().filter(|m| m.id != me.id) {
        let mut req = Request::new(JoinRequest { id: me.id });
        req.set_timeout(JOIN_TIMEOUT);
        match client(&m.url)?.join(req).await {
            Ok(_) => return Ok(()),
            Err(s) => last = anyhow::anyhow!("{}: {}", m.url, s.message()),
        }
    }
    Err(last)
}

fn client(url: &str) -> Result<ClusterClient<tonic::transport::Channel>> {
    Ok(ClusterClient::new(enroute_shared::auth::channel(url)?))
}

/// Admit node `id` as it announced itself: a member at that address and
/// key, through the root log.
async fn admit(engine: &Engine, objects: &Objects, id: u64) -> Result<(), Status> {
    let peer = announced(objects, id)
        .await
        .map_err(|e| Status::unavailable(format!("{e:#}")))?
        .ok_or_else(|| Status::failed_precondition(format!("node {id} did not announce itself")))?;
    if engine.members().1.contains(&id) {
        return Err(Status::failed_precondition(format!(
            "node {id} was removed from the ledger"
        )));
    }
    match engine.peer(id) {
        Some(p) if p == peer => return Ok(()),
        // Two copies of one log store could vote twice: refused while the
        // member answers at its address (a node that moved does not).
        Some(p) if p.url != peer.url && engine.answers(id) => {
            return Err(Status::failed_precondition(format!(
                "node {id} (its log store) is running at {}: a log store must run only once (stop it before moving its disk)",
                p.url
            )));
        }
        _ => {}
    }
    engine
        .propose_root(root_entry::Change::AddMember(peer).into())
        .await
        .map_err(|e| Status::unavailable(format!("{e:#}")))
}

/// The `Cluster` service of a ledger node (see `proto/peer.proto`).
pub(crate) struct ClusterService {
    pub(crate) engine: Arc<Engine>,
    pub(crate) objects: Arc<Objects>,
}

/// The `Cluster` service's server, for the internal router.
#[must_use]
pub fn server(
    engine: Arc<Engine>,
    objects: Arc<Objects>,
) -> cluster_server::ClusterServer<impl cluster_server::Cluster> {
    cluster_server::ClusterServer::new(ClusterService { engine, objects })
}

/// The `Cluster` service's name, which the internal router lets through
/// without a token.
pub const SERVICE_NAME: &str =
    <cluster_server::ClusterServer<ClusterService> as tonic::server::NamedService>::NAME;

#[tonic::async_trait]
impl cluster_server::Cluster for ClusterService {
    async fn join(&self, req: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        admit(&self.engine, &self.objects, req.into_inner().id).await?;
        Ok(Response::new(JoinResponse {}))
    }

    async fn remove(
        &self,
        req: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        // Only with a ledger node's own token (see `src/node.rs`).
        let RemoveRequest { id, force } = req.into_inner();
        let (members, removed) = self.engine.members();
        if removed.contains(&id) {
            return Ok(Response::new(RemoveResponse {}));
        }
        if !members.contains(&id) {
            return Err(Status::not_found(format!(
                "node {id} is not a member of the ledger"
            )));
        }
        if !force {
            leaves_majority(&members, &self.engine.alive(), id)
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
        }
        self.engine
            .propose_root(root_entry::Change::RemoveMember(id).into())
            .await
            .map_err(|e| Status::unavailable(format!("{e:#}")))?;
        Ok(Response::new(RemoveResponse {}))
    }

    async fn members(
        &self,
        _req: Request<MembersRequest>,
    ) -> Result<Response<MembersResponse>, Status> {
        let (members, removed) = self.engine.members();
        Ok(Response::new(MembersResponse {
            members,
            removed,
            alive: self.engine.alive(),
        }))
    }
}

/// Whether removing `id` from `members` leaves a majority of them alive.
fn leaves_majority(members: &[u64], alive: &[u64], id: u64) -> Result<()> {
    let rest: Vec<u64> = members.iter().copied().filter(|&x| x != id).collect();
    let live = rest.iter().filter(|x| alive.contains(x)).count();
    anyhow::ensure!(
        2 * live > rest.len(),
        "removing node {id} would leave {live} of {} members alive, not a majority (--force to remove it anyway)",
        rest.len()
    );
    Ok(())
}

/// The ledger's members, the nodes removed, and those alive.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Members {
    /// The members.
    pub members: Vec<u64>,
    /// The nodes removed for good.
    pub removed: Vec<u64>,
    /// The nodes alive, as the member that answered sees them.
    pub alive: Vec<u64>,
}

/// The ledger's members, as the first member that [`CLUSTER`] names and
/// that answers sees them (none if it names none).
///
/// # Errors
///
/// Returns an error if no member answers.
pub async fn members(objects: &Objects) -> Result<Members> {
    let named = named(objects).await?;
    if named.is_empty() {
        // No cluster (a standalone node), or none yet.
        return Ok(Members::default());
    }
    let mut last = anyhow::anyhow!("{CLUSTER} names no member");
    for Member { url, .. } in named {
        match client(&url)?.members(MembersRequest {}).await {
            Ok(r) => {
                let r = r.into_inner();
                return Ok(Members {
                    members: r.members,
                    removed: r.removed,
                    alive: r.alive,
                });
            }
            Err(s) => last = anyhow::anyhow!("{url}: {}", s.message()),
        }
    }
    Err(last)
}

/// Remove node `id` for good, through the root log, signed with the key
/// of the ledger node whose data directory is `data_dir` (so this runs on
/// a ledger node).
/// Refused, unless `force`, if the members left would have no live
/// majority.
///
/// # Errors
///
/// Returns an error if the key is no member's, node `id` is not a member,
/// the removal is refused, or no member answers.
pub async fn remove(objects: &Objects, data_dir: &Path, id: u64, force: bool) -> Result<Members> {
    let key = data_dir.join(token::KEY_FILE);
    let nodes = named(objects).await?;
    let token = token::own(
        &token::Issuer::load_of(&key, &nodes)?,
        Duration::from_secs(60),
    );
    let bearer: tonic::metadata::MetadataValue<_> = format!("Bearer {token}").parse()?;
    let mut last = anyhow::anyhow!("{CLUSTER} names no member");
    for Member { url, .. } in nodes {
        let mut req = Request::new(RemoveRequest { id, force });
        req.metadata_mut().insert("authorization", bearer.clone());
        match client(&url)?.remove(req).await {
            Ok(_) => return members(objects).await,
            Err(s)
                if matches!(
                    s.code(),
                    tonic::Code::FailedPrecondition | tonic::Code::NotFound
                ) =>
            {
                anyhow::bail!("{}", s.message())
            }
            Err(s) => last = anyhow::anyhow!("{url}: {}", s.message()),
        }
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A removal must leave a live majority.
    #[test]
    fn removals_keep_a_live_majority() {
        // Node 3 is down.
        leaves_majority(&[1, 2, 3], &[1, 2], 3).unwrap();
        let err = leaves_majority(&[1, 2, 3], &[1, 2], 1).unwrap_err();
        assert!(err.to_string().contains("1 of 2 members alive"), "{err:#}");
    }
}
