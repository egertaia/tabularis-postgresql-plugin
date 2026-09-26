//! Connections pinned to a host session (an editor tab).
//!
//! `execute_query_batch` already runs every statement of one batch on a
//! single pooled connection, so `BEGIN … COMMIT` inside a script works.
//! What did not work is the workflow the transaction exists for: run
//! `BEGIN`, inspect, run the changes, verify them, and only then `COMMIT`,
//! each as its own run. Between runs the connection went back to the pool,
//! so the next run could land on a different one and the transaction was
//! stranded.
//!
//! When the host sends a `session_id`, a batch that leaves a transaction
//! open keeps its connection here until the session commits, rolls back,
//! closes, or goes idle. The pool recycles with `RecyclingMethod::Fast`,
//! which resets nothing, so a pinned connection is always rolled back
//! before it goes back to the pool — otherwise the next borrower would
//! inherit the transaction and its locks.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use deadpool_postgres::Client;
use tokio::sync::Mutex;

/// A pooled client held between batches because the session that owns it
/// left an explicit transaction open.
struct PinnedSession {
    client: Client,
    last_used: Instant,
}

/// A pinned connection holds its transaction's locks until the session ends
/// it. An abandoned session would hold them indefinitely, so one untouched
/// for this long is rolled back and released by the periodic
/// [`sweep_idle`].
const MAX_IDLE: Duration = Duration::from_secs(30 * 60);

type SessionMap = HashMap<String, PinnedSession>;

fn sessions() -> &'static Mutex<SessionMap> {
    static SESSIONS: OnceLock<Mutex<SessionMap>> = OnceLock::new();
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// End the transaction before the client goes back to the pool.
///
/// A failing `ROLLBACK` is only logged: the connection is already unusable
/// and the pool will discard it.
pub async fn rollback_and_release(client: Client) {
    if let Err(e) = client.batch_execute("ROLLBACK").await {
        log::warn!("ROLLBACK while releasing a pinned session failed: {e}");
    }
}

/// Take the connection pinned to `session_id`, if any.
///
/// The caller owns the returned client and must either hand it back via
/// [`store`] or end the transaction itself.
pub async fn take(session_id: &str) -> Option<Client> {
    sessions().lock().await.remove(session_id).map(|s| s.client)
}

/// Roll back and release every session idle past [`MAX_IDLE`].
pub async fn sweep_idle() {
    let expired: Vec<Client> = {
        let mut map = sessions().lock().await;
        let now = Instant::now();
        let stale: Vec<String> = map
            .iter()
            .filter(|(_, s)| now.duration_since(s.last_used) > MAX_IDLE)
            .map(|(id, _)| id.clone())
            .collect();
        stale
            .iter()
            .filter_map(|id| map.remove(id).map(|s| s.client))
            .collect()
    };

    if expired.is_empty() {
        return;
    }
    log::info!(
        "Releasing {} pinned session(s) idle for over {} minutes",
        expired.len(),
        MAX_IDLE.as_secs() / 60
    );
    for client in expired {
        rollback_and_release(client).await;
    }
}

/// Pin `client` to `session_id` until the session ends its transaction.
pub async fn store(session_id: &str, client: Client) {
    let previous = {
        let mut map = sessions().lock().await;
        map.insert(
            session_id.to_string(),
            PinnedSession {
                client,
                last_used: Instant::now(),
            },
        )
    };

    // Only reachable if two batches for one session overlapped; the older
    // connection is no longer referenced by anything.
    if let Some(stale) = previous {
        rollback_and_release(stale.client).await;
    }
}

/// Roll back and release the connection pinned to `session_id`, if any.
pub async fn release(session_id: &str) {
    let client = {
        let mut map = sessions().lock().await;
        map.remove(session_id).map(|s| s.client)
    };

    if let Some(client) = client {
        log::info!("Releasing pinned session {session_id}");
        rollback_and_release(client).await;
    }
}

/// Roll back and release every pinned connection, so shutdown leaves no
/// transaction open on the server.
pub async fn release_all() {
    let clients: Vec<Client> = {
        let mut map = sessions().lock().await;
        map.drain().map(|(_, s)| s.client).collect()
    };

    if clients.is_empty() {
        return;
    }
    log::info!("Releasing {} pinned session(s)", clients.len());
    for client in clients {
        rollback_and_release(client).await;
    }
}
