//! Crash witness for [`Doc::set_bytes_durable`]'s cross-store ordering
//! contract: once the call returns, a hard kill can never leave the blob
//! store without the committed metadata for the content the call imported.
//!
//! The window this guards is real and measured. `add_bytes` is acknowledged
//! by the blob store's metadata actor while its redb write transaction is
//! still open; the commit lands later on the actor's batching schedule
//! (`max_write_batch` / timeout). A plain `set_bytes` inserts the document
//! record inside that window, so a SIGKILL between the two commits leaves a
//! durable record naming metadata that was never committed — reopening the
//! store reports the blob `NotFound` even though its data file may already be
//! on disk. With the barrier the reopened store reports `Complete` every
//! time. (Run the child in `DURABLE_CRASH_MODE=plain` to reproduce the
//! dangling state by hand; that mode is a diagnostic, not a CI assertion,
//! because the batched commit racing the kill makes its failure
//! probabilistic rather than certain.)
#![cfg(feature = "fs-store")]

use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
};

use n0_error::Result;

mod util;

/// Payload comfortably above the inline threshold so the entry's metadata
/// references an owned data file — the case where metadata commit and data
/// visibility genuinely decouple.
const PAYLOAD_LEN: usize = 8 * 1024 * 1024;
const READY_PREFIX: &str = "DURABLE_CRASH_READY ";

/// Child role, driven by the parent test below via `--exact --ignored`.
/// Writes one durable (or plain, per `DURABLE_CRASH_MODE`) entry into a
/// persistent node under `DURABLE_CRASH_DIR`, prints the content hash, and
/// parks until the parent kills it — the whole point is that this process
/// never shuts down cleanly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "child role for the crash witness; spawned by the parent test"]
async fn crash_child_writer() -> Result<()> {
    let dir = std::env::var("DURABLE_CRASH_DIR").expect("parent sets DURABLE_CRASH_DIR");
    let plain = std::env::var("DURABLE_CRASH_MODE").as_deref() == Ok("plain");

    let endpoint = util::empty_endpoint().await?;
    let node = util::Node::persistent(&dir, endpoint)
        .spawn()
        .await
        .unwrap();
    let author = node.docs().author_create().await?;
    let doc = node.docs().create().await?;

    let value = vec![0xA5u8; PAYLOAD_LEN];
    let hash = if plain {
        doc.set_bytes(author, "the-key", value).await?
    } else {
        doc.set_bytes_durable(author, "the-key", value).await?
    };

    // The parent kills us on this line; stdout must actually carry it.
    println!("{READY_PREFIX}{}", hash.to_hex());
    use std::io::Write;
    std::io::stdout().flush().ok();
    std::future::pending::<()>().await;
    unreachable!()
}

#[test]
fn a_hard_kill_after_set_bytes_durable_never_leaves_the_blob_uncommitted() {
    let exe = std::env::current_exe().expect("test exe");
    for round in 0..5 {
        let dir = tempfile::tempdir().expect("tempdir");

        // Forward the mode so `DURABLE_CRASH_MODE=plain cargo test --test
        // durable_set_crash` reproduces the dangling state as a diagnostic.
        let mode = std::env::var("DURABLE_CRASH_MODE").unwrap_or_else(|_| "durable".into());
        let mut child = Command::new(&exe)
            .args(["crash_child_writer", "--exact", "--ignored", "--nocapture"])
            .env("DURABLE_CRASH_DIR", dir.path())
            .env("DURABLE_CRASH_MODE", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child");

        // Kill the instant the durable write reports done: everything the
        // contract promises must already be on disk.
        let stdout = child.stdout.take().expect("child stdout");
        let mut hash_hex = None;
        for line in BufReader::new(stdout).lines() {
            let line = line.expect("child stdout line");
            if let Some(rest) = line.strip_prefix(READY_PREFIX) {
                hash_hex = Some(rest.trim().to_string());
                break;
            }
        }
        let hash_hex = hash_hex.expect("child printed the READY line before exiting");
        child.kill().expect("SIGKILL child");
        child.wait().expect("reap child");

        // Reopen the blob store alone (same layout as util::Node::persistent)
        // and demand committed, complete metadata for the hash.
        let hash: iroh_blobs::Hash = hash_hex.parse().expect("hash from child");
        let db_path = dir.path().join("blobs.db");
        let status = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(async move {
                let opts = iroh_blobs::store::fs::options::Options::new(dir.path());
                let store = iroh_blobs::store::fs::FsStore::load_with_opts(db_path, opts)
                    .await
                    .expect("reopen blob store after kill");
                let status = store.blobs().status(hash).await.expect("status");
                store.shutdown().await.ok();
                status
            });
        assert!(
            matches!(status, iroh_blobs::api::blobs::BlobStatus::Complete { .. }),
            "round {round}: reopened store reports {status:?} for the durably \
             published hash — the barrier failed to commit before the record",
        );
    }
}
