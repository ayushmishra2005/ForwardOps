use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::Value;

use crate::config::Bounds;
use crate::normalize::{gap, normalize_transaction, NormalizedRecord};
use crate::rpc::{with_retry, RpcError, SignatureInfo, SolanaSource};
use crate::storage::{Checkpoint, PendingSignature, Store, Window};
use crate::IngestError;

const MAX_SIGNATURE_PAGES: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReport {
    pub status: String,
    pub high_water_slot: Option<i64>,
    pub enumeration_complete: bool,
    pub stopped_reason: Option<String>,
    pub stored: i64,
    pub gaps: i64,
    pub pending: i64,
    pub records: i64,
    pub retries: u32,
    pub fetched: u32,
}

enum Resolved {
    Ready(NormalizedRecord),
    Unresolved { deadline: bool },
}

pub async fn run<S: SolanaSource>(
    source: &S,
    store: &mut Store,
    window: &Window,
    bounds: &Bounds,
) -> Result<RunReport, IngestError> {
    if !store.try_lock(window).await? {
        return Err(IngestError::Config(
            "another ingest run holds this window".into(),
        ));
    }
    let result = run_locked(source, store, window, bounds).await;
    if let Err(error) = store.unlock(window).await {
        if result.is_ok() {
            return Err(error);
        }
    }
    result
}

async fn run_locked<S: SolanaSource>(
    source: &S,
    store: &mut Store,
    window: &Window,
    bounds: &Bounds,
) -> Result<RunReport, IngestError> {
    let retries = AtomicU32::new(0);
    let mut fetched = 0u32;
    let mut checkpoint = store.prepare(window).await?;
    if checkpoint.status == "complete" {
        return report(store, window, &checkpoint, &retries, fetched).await;
    }

    let deadline = Instant::now() + bounds.deadline;
    let mut accepted = 0u32;
    let mut pages = 0usize;
    let mut before = checkpoint.signature_before.clone();
    while !checkpoint.enumeration_complete
        && accepted < bounds.max_transactions
        && pages < MAX_SIGNATURE_PAGES
    {
        if Instant::now() >= deadline {
            checkpoint = store.note(window, "deadline", true).await?;
            return report(store, window, &checkpoint, &retries, fetched).await;
        }
        pages += 1;
        let page = match with_retry(deadline, bounds.max_retries, &retries, || {
            source.signature_page(&window.address, before.as_deref())
        })
        .await
        {
            Ok(page) => page,
            Err(_) => {
                checkpoint = store.note(window, "page_failed", true).await?;
                return report(store, window, &checkpoint, &retries, fetched).await;
            }
        };
        if page.is_empty() {
            checkpoint = store
                .commit_page(window, &[], before.as_deref(), true, None)
                .await?;
            break;
        }
        let (keep, cursor, crossed_below, reached_cap) =
            select_signatures(&page, &before, window, accepted, bounds.max_transactions);
        if !crossed_below && cursor.as_deref() == before.as_deref() {
            checkpoint = store.note(window, "page_failed", true).await?;
            return report(store, window, &checkpoint, &retries, fetched).await;
        }
        accepted += keep.len() as u32;
        let reason = if reached_cap && !crossed_below {
            Some("max_transactions")
        } else {
            None
        };
        checkpoint = store
            .commit_page(window, &keep, cursor.as_deref(), crossed_below, reason)
            .await?;
        before = checkpoint.signature_before.clone();
        if crossed_below || reached_cap {
            break;
        }
    }
    if !checkpoint.enumeration_complete
        && accepted < bounds.max_transactions
        && pages >= MAX_SIGNATURE_PAGES
    {
        checkpoint = store.note(window, "page_budget", false).await?;
    }

    let pending = store
        .pending(window, i64::from(bounds.max_transactions))
        .await?;
    for chunk in pending.chunks(bounds.concurrency.max(1)) {
        if Instant::now() >= deadline {
            checkpoint = store.note(window, "deadline", true).await?;
            return report(store, window, &checkpoint, &retries, fetched).await;
        }
        let resolved = map_limited(chunk, bounds.concurrency, |item| {
            fetch_one(source, item, deadline, bounds.max_retries, &retries)
        })
        .await;
        let mut ready = Vec::new();
        let mut unresolved_deadline = false;
        let mut unresolved = false;
        for item in resolved {
            fetched += 1;
            match item {
                Resolved::Ready(record) => ready.push(record),
                Resolved::Unresolved { deadline: true } => unresolved_deadline = true,
                Resolved::Unresolved { deadline: false } => unresolved = true,
            }
        }
        if !ready.is_empty() {
            checkpoint = store.commit_records(window, &ready).await?;
        }
        if unresolved_deadline || unresolved {
            let reason = if unresolved_deadline || Instant::now() >= deadline {
                "deadline"
            } else {
                "retry_exhausted"
            };
            checkpoint = store.note(window, reason, true).await?;
            return report(store, window, &checkpoint, &retries, fetched).await;
        }
    }
    report(store, window, &checkpoint, &retries, fetched).await
}

fn select_signatures(
    page: &[SignatureInfo],
    before: &Option<String>,
    window: &Window,
    accepted: u32,
    max_transactions: u32,
) -> (Vec<SignatureInfo>, Option<String>, bool, bool) {
    let mut keep = Vec::new();
    let mut cursor = before.clone();
    let mut crossed_below = false;
    let mut seen = HashSet::new();
    for item in page {
        if !seen.insert(item.signature.clone()) {
            continue;
        }
        if item.slot > window.to_slot {
            cursor = Some(item.signature.clone());
            continue;
        }
        if item.slot < window.from_slot {
            crossed_below = true;
            break;
        }
        if accepted + keep.len() as u32 >= max_transactions {
            break;
        }
        cursor = Some(item.signature.clone());
        keep.push(item.clone());
    }
    let reached_cap = accepted + keep.len() as u32 >= max_transactions && !crossed_below;
    (keep, cursor, crossed_below, reached_cap)
}

async fn fetch_one<S: SolanaSource>(
    source: &S,
    item: PendingSignature,
    deadline: Instant,
    max_retries: u32,
    retries: &AtomicU32,
) -> Resolved {
    let signature = item.signature.clone();
    let fetched = with_retry(deadline, max_retries, retries, || {
        source.transaction(&signature)
    })
    .await;
    decide(&item, fetched)
}

fn decide(item: &PendingSignature, fetched: Result<Option<Value>, RpcError>) -> Resolved {
    match fetched {
        Ok(Some(body)) if body.is_object() => Resolved::Ready(normalize_transaction(
            &item.signature,
            item.slot,
            item.block_time_unix,
            &body,
        )),
        Ok(Some(_)) => Resolved::Ready(gap(
            &item.signature,
            item.slot,
            item.block_time_unix,
            "malformed",
            "malformed transaction response",
        )),
        Ok(None) => Resolved::Ready(gap(
            &item.signature,
            item.slot,
            item.block_time_unix,
            "not_found",
            "no transaction at this commitment",
        )),
        Err(RpcError::Fatal(message))
            if message
                .to_ascii_lowercase()
                .contains("unsupported transaction version") =>
        {
            Resolved::Ready(gap(
                &item.signature,
                item.slot,
                item.block_time_unix,
                "unsupported_version",
                &message,
            ))
        }
        Err(RpcError::Fatal(message)) if message.contains("run deadline elapsed") => {
            Resolved::Unresolved { deadline: true }
        }
        Err(_) => Resolved::Unresolved { deadline: false },
    }
}

async fn report(
    store: &mut Store,
    window: &Window,
    checkpoint: &Checkpoint,
    retries: &AtomicU32,
    fetched: u32,
) -> Result<RunReport, IngestError> {
    let counts = store.counts(window).await?;
    Ok(RunReport {
        status: checkpoint.status.clone(),
        high_water_slot: checkpoint.high_water_slot,
        enumeration_complete: checkpoint.enumeration_complete,
        stopped_reason: checkpoint.stopped_reason.clone(),
        stored: counts.stored,
        gaps: counts.gaps,
        pending: counts.pending,
        records: counts.records,
        retries: retries.load(Ordering::Relaxed),
        fetched,
    })
}

async fn map_limited<T, R, F, Fut>(items: &[T], limit: usize, f: F) -> Vec<R>
where
    T: Clone,
    F: Fn(T) -> Fut,
    Fut: Future<Output = R>,
{
    let limit = limit.max(1);
    let f = &f;
    let mut incoming = items.iter().cloned().enumerate();
    let mut pending = FuturesUnordered::new();
    for _ in 0..limit {
        if let Some((index, item)) = incoming.next() {
            pending.push(apply(index, item, f));
        }
    }
    let mut indexed = Vec::with_capacity(items.len());
    while let Some((index, value)) = pending.next().await {
        indexed.push((index, value));
        if let Some((next_index, item)) = incoming.next() {
            pending.push(apply(next_index, item, f));
        }
    }
    indexed.sort_by_key(|(index, _)| *index);
    indexed.into_iter().map(|(_, value)| value).collect()
}

async fn apply<T, R, F, Fut>(index: usize, item: T, f: &F) -> (usize, R)
where
    F: Fn(T) -> Fut,
    Fut: Future<Output = R>,
{
    (index, f(item).await)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::json;

    use super::{decide, map_limited, Resolved};
    use crate::rpc::RpcError;
    use crate::storage::PendingSignature;

    fn pending() -> PendingSignature {
        PendingSignature {
            signature: "sig".into(),
            slot: 4,
            block_time_unix: None,
        }
    }

    #[test]
    fn not_found_malformed_and_version_errors_are_explicit_gaps() {
        match decide(&pending(), Ok(None)) {
            Resolved::Ready(record) => {
                assert_eq!(record.outcome, "not_found");
                assert!(record.gap_reason.unwrap().contains("commitment"));
            }
            Resolved::Unresolved { .. } => panic!("not found must be recorded"),
        }
        match decide(&pending(), Ok(Some(json!("not-a-transaction")))) {
            Resolved::Ready(record) => assert_eq!(record.outcome, "malformed"),
            Resolved::Unresolved { .. } => panic!("malformed body must be recorded"),
        }
        match decide(
            &pending(),
            Err(RpcError::Fatal("unsupported transaction version: 1".into())),
        ) {
            Resolved::Ready(record) => assert_eq!(record.outcome, "unsupported_version"),
            Resolved::Unresolved { .. } => panic!("version error must be recorded"),
        }
    }

    #[test]
    fn retryable_failures_stay_unresolved() {
        let outcome = decide(
            &pending(),
            Err(RpcError::Retryable {
                retry_after: None,
                message: "429".into(),
            }),
        );
        assert!(matches!(outcome, Resolved::Unresolved { deadline: false }));
    }

    #[tokio::test]
    async fn concurrency_stays_inside_the_limit() {
        let inflight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let items = vec![1_u8, 2, 3, 4, 5, 6];
        let inflight_counter = inflight.clone();
        let peak_counter = peak.clone();
        let results = map_limited(&items, 2, move |item| {
            let inflight_counter = inflight_counter.clone();
            let peak_counter = peak_counter.clone();
            async move {
                let now = inflight_counter.fetch_add(1, Ordering::SeqCst) + 1;
                peak_counter.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(40)).await;
                inflight_counter.fetch_sub(1, Ordering::SeqCst);
                item
            }
        })
        .await;
        assert_eq!(results, items);
        let highest = peak.load(Ordering::SeqCst);
        assert!(highest <= 2, "peak {highest}");
        assert!(highest >= 2, "peak {highest}");
    }

    #[test]
    fn the_transaction_cap_leaves_the_rest_of_the_page_for_resume() {
        let page = vec![
            signature("newer", 30),
            signature("older", 20),
            signature("oldest", 16),
        ];
        let window = sample_window(15, 40);
        let (keep, cursor, crossed, reached_cap) =
            super::select_signatures(&page, &None, &window, 0, 1);
        assert_eq!(keep.len(), 1);
        assert_eq!(keep[0].signature, "newer");
        assert_eq!(cursor.as_deref(), Some("newer"));
        assert!(!crossed);
        assert!(reached_cap);
        let (rest, _, crossed, _) = super::select_signatures(&page[1..], &cursor, &window, 0, 10);
        assert_eq!(rest.len(), 2);
        assert!(!crossed);
    }

    #[test]
    fn a_signature_below_the_window_completes_enumeration() {
        let page = vec![signature("inside", 30), signature("below", 5)];
        let (keep, _, crossed, reached_cap) =
            super::select_signatures(&page, &None, &sample_window(10, 40), 0, 10);
        assert_eq!(keep.len(), 1);
        assert!(crossed);
        assert!(!reached_cap);
    }

    fn signature(name: &str, slot: i64) -> crate::rpc::SignatureInfo {
        crate::rpc::SignatureInfo {
            signature: name.into(),
            slot,
            block_time: None,
        }
    }

    fn sample_window(from_slot: i64, to_slot: i64) -> crate::storage::Window {
        crate::storage::Window {
            source_id: "solana-mainnet".into(),
            cluster_id: "mainnet-beta".into(),
            address: "address".into(),
            commitment: "finalized".into(),
            from_slot,
            to_slot,
        }
    }
}
