//! Reproduction: a Connect-origin sync that ends in `RemoteAbort(AlreadySyncing)`
//! wedges the loser's `PeerState` permanently when the winner's reciprocal
//! connection never arrives.
//!
//! `engine/live.rs::on_sync_via_connect_finished` handles that abort by only
//! logging — `PeerState::finish()` is never called, the state stays
//! `Running{Connect}`, and every later `start_connect` for that (namespace,
//! peer) is refused with "sync already running". The design bet is that the
//! tie-break winner's connection will complete on our accept side and clear
//! the state; when the winner cannot reach us (observed in production after a
//! peer restart changed its address: the winner re-dialed a stale address for
//! minutes), nothing ever resets the loser — no timeout, no retry, and the
//! `resync_requested` flag is only consumed inside the `finish()` that never
//! runs.
//!
//! Production trace: portal-sync bootstrap-union campaign 2026-08-24 — the
//! loser logged `abort connect: sync already running` 71 times over 2.5
//! minutes with zero accept-side spans, while the winner logged
//! `Failed to establish connection` every ~30s.
//!
//! The staging here makes the race deterministic:
//! - keys are chosen so `wedged.id() > winner.id()` — by
//!   `expected_sync_direction`, the winner then REJECTS the wedged node's
//!   incoming request whenever the winner's own connect attempt is running,
//!   and the wedged node would accept the winner's;
//! - the winner is given a blackhole address for the wedged node (TEST-NET-1,
//!   a UDP hole), so its connect attempt is Running for the whole handshake
//!   timeout — the collision window — and can never complete, which is the
//!   "stale address after restart" condition;
//! - the wedged node does NOT register the docs ALPN acceptor, so no later
//!   accept-side sync can accidentally clear its state — the only route to
//!   convergence is the wedged node's own Connect path, the one the bug
//!   disables.
//!
//! The assertion is the *correct* behaviour: an entry written on the
//! reachable winner must arrive at the wedged node, which keeps asking for
//! sync with a good address. On iroh-docs 0.101.3 this test FAILS (the entry
//! never arrives); a fix that stops treating `RemoteAbort(AlreadySyncing)` as
//! "someone else will finish this, forever" makes it pass.

use anyhow::Result;
use iroh::{endpoint::presets, Endpoint, EndpointAddr, SecretKey, TransportAddr};
use iroh_docs::{
    api::protocol::{AddrInfoOptions, ShareMode},
    engine::LiveEvent,
    AuthorId,
};
use iroh_gossip::net::Gossip;
use n0_future::{time::Duration, StreamExt};
use rand::{RngExt, SeedableRng};
use tracing::info;

mod util;

/// A key pair such that `first.public() > second.public()` as byte strings —
/// `expected_sync_direction` then makes `first` the accept side of a
/// simultaneous dial, i.e. the node whose own connect gets rejected with
/// `AlreadySyncing` (the wedge candidate).
fn ordered_keys(seed: &[u8]) -> (SecretKey, SecretKey) {
    let mut rng = rand::rngs::ChaCha12Rng::from_seed(*iroh_blobs::Hash::new(seed).as_bytes());
    loop {
        let a = SecretKey::from_bytes(&rng.random());
        let b = SecretKey::from_bytes(&rng.random());
        if a.public().as_bytes() > b.public().as_bytes() {
            return (a, b);
        }
    }
}

#[tokio::test]
async fn connect_rejected_as_already_syncing_must_not_wedge_forever() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "iroh_docs=debug,sync_alreadysyncing_wedge=info".into()),
        )
        .with_test_writer()
        .try_init();
    let (wedge_key, winner_key) = ordered_keys(b"alreadysyncing-wedge");

    // The wedge candidate: a full docs engine that can DIAL, but does not
    // accept the docs ALPN — sync toward it can only be initiated by it.
    let wedged_ep = Endpoint::builder(presets::Minimal)
        .secret_key(wedge_key)
        .bind()
        .await?;
    let wedged_store = iroh_blobs::store::mem::MemStore::new();
    let wedged_gossip = Gossip::builder().spawn(wedged_ep.clone());
    let wedged_docs = iroh_docs::protocol::Docs::memory()
        .spawn(wedged_ep.clone(), (*wedged_store).clone(), wedged_gossip.clone())
        .await?;
    // Deliberately NO `.accept(iroh_docs::ALPN, …)`: see module docs.
    let wedged_router = iroh::protocol::Router::builder(wedged_ep.clone())
        .accept(
            iroh_blobs::ALPN,
            iroh_blobs::BlobsProtocol::new(&wedged_store, None),
        )
        .accept(iroh_gossip::ALPN, wedged_gossip.clone())
        .spawn();

    // The winner: an ordinary full node (accepts docs).
    let winner = {
        let ep = Endpoint::builder(presets::Minimal)
            .secret_key(winner_key)
            .bind()
            .await?;
        util::Node::memory(ep).spawn().await?
    };

    info!(wedged = %wedged_ep.id().fmt_short(), winner = %winner.id().fmt_short(), "topology");
    assert!(
        wedged_ep.id().as_bytes() > winner.id().as_bytes(),
        "key crafting broke: the wedge candidate must win the accept tie-break",
    );

    // One shared namespace: created on the winner, imported on the wedge
    // candidate WITHOUT auto-sync (the collision below is staged by hand).
    // The write-mode ticket carries the winner's real addresses.
    let doc_winner = winner.docs().create().await?;
    let ticket = doc_winner
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?;
    let winner_addr = ticket
        .nodes
        .first()
        .cloned()
        .expect("a RelayAndAddresses ticket names its node");
    let doc_wedged = wedged_docs
        .api()
        .import_namespace(ticket.capability.clone())
        .await?;
    let author_w: AuthorId = winner.docs().author_create().await?;

    // Stage the collision: the winner dials a blackhole "address" for the
    // wedge candidate (stale-address stand-in; UDP into TEST-NET-1 never
    // answers, so the attempt stays Running for the whole handshake
    // timeout)…
    let blackhole = EndpointAddr::from_parts(
        wedged_ep.id(),
        [TransportAddr::Ip("192.0.2.1:9999".parse().unwrap())],
    );
    doc_winner.start_sync(vec![blackhole]).await?;
    n0_future::time::sleep(Duration::from_millis(300)).await;

    // …and while that attempt is running, the wedge candidate dials the
    // winner's REAL address. The winner's accept sees its own connect
    // running, and the tie-break says "I connect, you accept" — so it
    // rejects this request with `AlreadySyncing`. The wedge candidate's
    // engine keeps its state Running forever after that rejection.
    let mut wedged_events = doc_wedged.subscribe().await?;
    doc_wedged.start_sync(vec![winner_addr.clone()]).await?;

    // Let the collision resolve: the winner's blackhole attempt times out
    // and its engine goes idle (it holds no usable address for the wedge
    // candidate, exactly like the production winner that kept failing to
    // establish).
    n0_future::time::sleep(Duration::from_secs(3)).await;

    // The winner writes an entry the wedge candidate needs.
    let _hash = doc_winner
        .set_bytes(author_w, b"after-collision".to_vec(), b"payload".to_vec())
        .await?;

    // The wedge candidate keeps asking for sync with a perfectly good
    // address. The discriminating observable is a SYNC ROUND ENDING on the
    // wedge candidate (`LiveEvent::SyncFinished`): entry *metadata* can also
    // arrive over the gossip broadcast, which bypasses the sync state machine
    // entirely and therefore proves nothing about it — an earlier revision of
    // this test asserted on `InsertRemote` and passed spuriously through
    // exactly that side door. A healthy engine turns any one of these
    // dispatches into a round against a reachable peer within milliseconds;
    // the wedged engine refuses every one as "sync already running" and no
    // round ever starts, let alone finishes.
    let deadline = n0_future::time::Instant::now() + Duration::from_secs(60);
    let mut round_ended = false; // a SUCCESSFUL round, not merely an ended one
    'outer: while n0_future::time::Instant::now() < deadline {
        doc_wedged.start_sync(vec![winner_addr.clone()]).await?;
        let step = n0_future::time::sleep(Duration::from_millis(500));
        tokio::pin!(step);
        loop {
            tokio::select! {
                _ = &mut step => break,
                ev = wedged_events.next() => {
                    match ev {
                        Some(Ok(LiveEvent::SyncFinished(ev))) => {
                            info!(?ev, "sync round ended on the wedge candidate");
                            // Only a round that actually RECONCILED counts: the
                            // fix also surfaces the aborted collision round as
                            // a failed SyncFinished, and accepting that would
                            // pass the test without any recovery happening.
                            if ev.result.is_ok() {
                                round_ended = true;
                                break 'outer;
                            }
                        }
                        Some(Ok(other)) => info!(?other, "event"),
                        Some(Err(e)) => info!("event error: {e:#}"),
                        None => break,
                    }
                }
            }
        }
    }

    assert!(
        round_ended,
        "no sync round ever ran on a node that requested sync with a reachable peer every \
         500ms for 60s: the RemoteAbort(AlreadySyncing) rejection left its PeerState \
         Running forever (engine/live.rs swallows the abort without finish()), so every \
         dispatch was refused as 'sync already running'",
    );

    winner.shutdown().await?;
    wedged_router.shutdown().await?;
    Ok(())
}
