//! Shared test scaffolding: an in-process transport and ready-made nodes.
//!
//! The transport is deliberately dumb (a pair of channels) so that every
//! behaviour the tests observe comes from DBMesh and not from the transport.
//! It can be made to fail mid-session, and it records every frame sent so
//! tests can assert on what actually crossed the wire.

#![allow(dead_code, clippy::missing_panics_doc)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use dbmesh::adapter::MemoryDatabase;
use dbmesh::core::{
    Capability, Collection, ConflictResolver, ConnectionInfo, Mutation, Operation, Payload, Peer,
    PeerId, RecordId, RejectOnConflict, StaticPolicy, StaticTrust, TransportCapability,
    TransportKind,
};
use dbmesh::protocol::{Decoded, Frame, Message, codec};
use dbmesh::storage::MemoryStore;
use dbmesh::transport::{Acceptor, Connection, Transport};
use dbmesh::{DbMesh, Error, Result, SessionReport};
use serde_json::json;
use tokio::sync::mpsc;

/// Every frame that crossed the wire, in the order it was sent.
#[derive(Clone, Default)]
pub struct Wire(Arc<Mutex<Vec<(String, Frame)>>>);

impl Wire {
    /// Empties the log and returns what it held.
    pub fn take(&self) -> Vec<(String, Frame)> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }

    /// The messages sent by `from`, emptying the log.
    pub fn sent_by(&self, from: &str) -> Vec<Message> {
        self.take()
            .into_iter()
            .filter(|(sender, _)| sender == from)
            .map(|(_, frame)| frame.message)
            .collect()
    }
}

/// One end of an in-process connection.
pub struct ChannelConnection {
    name: String,
    info: ConnectionInfo,
    tx: Option<mpsc::UnboundedSender<Vec<u8>>>,
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    wire: Wire,
    // How many more sends succeed before the connection breaks. `None` is unlimited.
    budget: Option<usize>,
    severed: bool,
}

fn info() -> ConnectionInfo {
    ConnectionInfo {
        transport: TransportKind::new("channel").unwrap(),
        capabilities: BTreeSet::from([TransportCapability::Reliable, TransportCapability::Ordered]),
        remote: None,
    }
}

/// A connected pair. `from_budget` limits how many frames the first end may send.
pub fn pair(
    from: &str,
    to: &str,
    wire: &Wire,
    from_budget: Option<usize>,
) -> (ChannelConnection, ChannelConnection) {
    let (a_tx, b_rx) = mpsc::unbounded_channel();
    let (b_tx, a_rx) = mpsc::unbounded_channel();
    let end = |name: &str, tx, rx, budget| ChannelConnection {
        name: name.to_owned(),
        info: info(),
        tx: Some(tx),
        rx,
        wire: wire.clone(),
        budget,
        severed: false,
    };
    (
        end(from, a_tx, a_rx, from_budget),
        end(to, b_tx, b_rx, None),
    )
}

impl Connection for ChannelConnection {
    fn info(&self) -> &ConnectionInfo {
        &self.info
    }

    async fn send(&mut self, message: Vec<u8>) -> Result<()> {
        if let Some(budget) = &mut self.budget {
            if *budget == 0 {
                // The cable is cut: both directions stop, as in a real broken connection.
                self.severed = true;
                self.tx = None;
                return Err(Error::Transport("connection severed by the test".into()));
            }
            *budget -= 1;
        }
        let Decoded::Frame(frame) = codec::decode(&message)? else {
            panic!("tests only send frames this build understands");
        };
        self.wire.0.lock().unwrap().push((self.name.clone(), frame));
        self.tx
            .as_ref()
            .ok_or_else(|| Error::Transport("closed".into()))?
            .send(message)
            .map_err(|e| Error::Transport(Box::new(e)))
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        if self.severed {
            return Err(Error::Transport("connection severed by the test".into()));
        }
        Ok(self.rx.recv().await)
    }

    async fn close(&mut self) -> Result<()> {
        self.tx = None;
        Ok(())
    }
}

/// A set of listening nodes that dialers can reach, and that can be taken offline.
#[derive(Clone, Default)]
pub struct Network {
    inboxes: Arc<Mutex<BTreeMap<String, mpsc::UnboundedSender<ChannelConnection>>>>,
    offline: Arc<Mutex<BTreeSet<String>>>,
    pub wire: Wire,
}

/// Where a node receives incoming connections.
pub struct Listener(mpsc::UnboundedReceiver<ChannelConnection>);

impl Acceptor for Listener {
    type Connection = ChannelConnection;

    async fn accept(&mut self) -> Result<ChannelConnection> {
        self.0
            .recv()
            .await
            .ok_or_else(|| Error::Transport("the network is gone".into()))
    }
}

/// Dials from one node into the [`Network`].
pub struct Dialer {
    from: String,
    network: Network,
    // Applied to the next connection only.
    next_budget: Mutex<Option<usize>>,
}

impl Network {
    pub fn listen(&self, name: &str) -> Listener {
        let (tx, rx) = mpsc::unbounded_channel();
        self.inboxes.lock().unwrap().insert(name.to_owned(), tx);
        Listener(rx)
    }

    pub fn dialer(&self, from: &str) -> Dialer {
        Dialer {
            from: from.to_owned(),
            network: self.clone(),
            next_budget: Mutex::new(None),
        }
    }

    pub fn set_offline(&self, name: &str, offline: bool) {
        let mut set = self.offline.lock().unwrap();
        if offline {
            set.insert(name.to_owned());
        } else {
            set.remove(name);
        }
    }
}

impl Dialer {
    /// Breaks the next connection after it has sent `frames` frames.
    pub fn sever_next_after(&self, frames: usize) {
        *self.next_budget.lock().unwrap() = Some(frames);
    }
}

impl Transport for Dialer {
    type Connection = ChannelConnection;

    fn capabilities(&self) -> BTreeSet<TransportCapability> {
        info().capabilities
    }

    async fn connect(&self, peer: &Peer) -> Result<ChannelConnection> {
        let target = peer.id.as_str();
        let unreachable =
            |why: &str| Error::Transport(format!("{target} is unreachable: {why}").into());
        if self.network.offline.lock().unwrap().contains(target)
            || self.network.offline.lock().unwrap().contains(&self.from)
        {
            return Err(unreachable("offline"));
        }
        let inbox = self
            .network
            .inboxes
            .lock()
            .unwrap()
            .get(target)
            .cloned()
            .ok_or_else(|| unreachable("not listening"))?;
        let budget = self.next_budget.lock().unwrap().take();
        let (client, server) = pair(&self.from, target, &self.network.wire, budget);
        inbox.send(server).map_err(|_| unreachable("gone"))?;
        Ok(client)
    }
}

/// The mesh type used throughout the tests.
pub type TestMesh<R = RejectOnConflict> =
    DbMesh<MemoryDatabase, MemoryStore, StaticPolicy, R, StaticTrust>;

/// A node together with the pieces tests need to poke at.
pub struct Node<R = RejectOnConflict> {
    pub name: String,
    pub mesh: TestMesh<R>,
    pub db: MemoryDatabase,
    pub store: MemoryStore,
    pub listener: Listener,
}

pub fn collection(name: &str) -> Collection {
    Collection::new(name).unwrap()
}

pub fn peer_id(name: &str) -> PeerId {
    PeerId::new(name).unwrap()
}

pub fn record(table: &str, key: &str) -> RecordId {
    RecordId::new(collection(table), key)
}

/// A create mutation of `table:key` holding `{"v": value}`.
pub fn create(table: &str, key: &str, value: i64) -> Mutation {
    Mutation::new(
        record(table, key),
        Operation::Create,
        Some(Payload(json!({ "v": value }))),
    )
}

pub fn value(db: &MemoryDatabase, table: &str, key: &str) -> Option<i64> {
    db.get(&record(table, key))
        .map(|p| p.0["v"].as_i64().unwrap())
}

/// Builds a node that trusts and shares `collections` with each peer in `peers`.
pub async fn node(network: &Network, name: &str, peers: &[&str], collections: &[&str]) -> Node {
    node_with(
        network,
        name,
        peers,
        collections,
        RejectOnConflict,
        MemoryDatabase::new(),
        MemoryStore::new(),
    )
    .await
}

pub async fn node_with<R: ConflictResolver>(
    network: &Network,
    name: &str,
    peers: &[&str],
    collections: &[&str],
    resolver: R,
    db: MemoryDatabase,
    store: MemoryStore,
) -> Node<R> {
    let mut policy = StaticPolicy::new();
    let mut trust = StaticTrust::new();
    for peer in peers {
        policy = policy.exchange(
            peer_id(peer),
            collections
                .iter()
                .map(|c| collection(c))
                .collect::<Vec<_>>(),
        );
        trust = trust.trust(peer_id(peer), [Capability::relay()]);
    }
    let mesh = DbMesh::builder(db.clone(), store.clone())
        .identity(dbmesh::core::NodeId::new(name).unwrap())
        .policy(policy)
        .resolver(resolver)
        .security(trust)
        .batch_limit(2)
        .build()
        .await
        .unwrap();
    for peer in peers {
        mesh.register_peer(Peer::new(peer_id(peer))).await.unwrap();
    }
    Node {
        name: name.to_owned(),
        mesh,
        db,
        store,
        listener: network.listen(name),
    }
}

/// Runs one session: `from` dials `to`, and `to` serves the connection.
pub async fn sync<R1: ConflictResolver, R2: ConflictResolver>(
    network: &Network,
    from: &Node<R1>,
    to: &mut Node<R2>,
) -> (Result<SessionReport>, Result<SessionReport>) {
    let dialer = network.dialer(&from.name);
    sync_over(&dialer, from, to).await
}

pub async fn sync_over<R1: ConflictResolver, R2: ConflictResolver>(
    dialer: &Dialer,
    from: &Node<R1>,
    to: &mut Node<R2>,
) -> (Result<SessionReport>, Result<SessionReport>) {
    let target = peer_id(&to.name);
    let mesh = &to.mesh;
    let listener = &mut to.listener;
    tokio::join!(from.mesh.sync_with(dialer, &target), async {
        let connection = listener.accept().await.unwrap();
        mesh.serve(connection).await
    })
}
