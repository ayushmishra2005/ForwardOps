use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use serde_json::{json, Value};
use tokio_postgres::NoTls;

use forwardops_solana_ingest::config::Bounds;
use forwardops_solana_ingest::ingest::run;
use forwardops_solana_ingest::rpc::{RpcError, SignatureInfo, SolanaSource};
use forwardops_solana_ingest::storage::{Store, Window};

struct Page {
    before: Option<String>,
    result: Result<Vec<SignatureInfo>, RpcError>,
}

struct Script {
    pages: Mutex<VecDeque<Page>>,
    transactions: Mutex<HashMap<String, Result<Option<Value>, RpcError>>>,
    inflight: AtomicUsize,
    peak: AtomicUsize,
}

impl SolanaSource for Script {
    async fn signature_page(
        &self,
        _address: &str,
        before: Option<&str>,
    ) -> Result<Vec<SignatureInfo>, RpcError> {
        let page = self
            .pages
            .lock()
            .expect("pages")
            .pop_front()
            .expect("unexpected signature page");
        assert_eq!(page.before.as_deref(), before);
        page.result
    }

    async fn transaction(&self, signature: &str) -> Result<Option<Value>, RpcError> {
        let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let result = self
            .transactions
            .lock()
            .expect("transactions")
            .remove(signature)
            .unwrap_or_else(|| panic!("unexpected transaction {signature}"));
        tokio::time::sleep(Duration::from_millis(30)).await;
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

#[tokio::test]
async fn interrupted_run_resumes_without_duplicates_or_skipped_pages() {
    let Ok(admin_url) = std::env::var("TEST_DATABASE_ADMIN_URL") else {
        eprintln!("skipping postgres ingest test: TEST_DATABASE_ADMIN_URL is unset");
        return;
    };
    let database_url = prepare_database(&admin_url).await;
    let mut store = Store::connect(&database_url).await.expect("connect");
    let primary = window(10, 30);
    let primary_bounds = bounds(10, 30, 10);
    let older = signature(2);
    let newer = signature(1);

    let failed_page = Script::new(
        vec![
            Page {
                before: None,
                result: Ok(vec![info(&newer, 20), info(&older, 12)]),
            },
            Page {
                before: Some(older.clone()),
                result: Err(RpcError::Retryable {
                    retry_after: None,
                    message: "429".into(),
                }),
            },
        ],
        vec![],
    );
    let first = run(&failed_page, &mut store, &primary, &primary_bounds)
        .await
        .expect("first run");
    assert_eq!(first.status, "blocked");
    assert_eq!(first.stopped_reason.as_deref(), Some("page_failed"));
    assert!(!first.enumeration_complete);
    assert_eq!(first.high_water_slot, None);
    assert_eq!(first.pending, 2);
    assert_eq!(first.records, 0);
    assert_eq!(first.fetched, 0);

    let resume_pages = Script::new(
        vec![Page {
            before: Some(older.clone()),
            result: Ok(vec![]),
        }],
        vec![
            (newer.clone(), Ok(Some(body(20)))),
            (
                older.clone(),
                Err(RpcError::Retryable {
                    retry_after: None,
                    message: "timeout".into(),
                }),
            ),
        ],
    );
    let second = run(&resume_pages, &mut store, &primary, &primary_bounds)
        .await
        .expect("second run");
    assert_eq!(second.status, "blocked");
    assert_eq!(second.stopped_reason.as_deref(), Some("retry_exhausted"));
    assert!(second.enumeration_complete);
    assert_eq!(second.high_water_slot, Some(11));
    assert_eq!(second.stored, 1);
    assert_eq!(second.pending, 1);
    assert_eq!(second.records, 1);
    let peak = resume_pages.peak.load(Ordering::SeqCst);
    assert_eq!(peak, 2);

    let finish = Script::new(vec![], vec![(older.clone(), Ok(Some(body(12))))]);
    let third = run(&finish, &mut store, &primary, &primary_bounds)
        .await
        .expect("third run");
    assert_eq!(third.status, "complete");
    assert_eq!(third.high_water_slot, Some(30));
    assert_eq!(third.records, 2);
    assert_eq!(third.pending, 0);

    mark_pending(&database_url, &newer).await;
    let again = Script::new(vec![], vec![(newer.clone(), Ok(Some(body(20))))]);
    let duplicate = run(&again, &mut store, &primary, &primary_bounds)
        .await
        .expect("duplicate run");
    assert_eq!(duplicate.status, "complete");
    assert_eq!(duplicate.records, 2);
    assert_eq!(stored_rows(&database_url, &primary).await, 2);

    let missing = signature(3);
    let malformed = signature(4);
    let unsupported = signature(5);
    let gaps = window(40, 50);
    let gap_run = Script::new(
        vec![
            Page {
                before: None,
                result: Ok(vec![
                    info(&missing, 45),
                    info(&malformed, 42),
                    info(&unsupported, 41),
                ]),
            },
            Page {
                before: Some(unsupported.clone()),
                result: Ok(vec![]),
            },
        ],
        vec![
            (missing.clone(), Ok(None)),
            (malformed.clone(), Ok(Some(json!([])))),
            (
                unsupported.clone(),
                Err(RpcError::Fatal("unsupported transaction version: 1".into())),
            ),
        ],
    );
    let gap_bounds = bounds(40, 50, 10);
    let recorded = run(&gap_run, &mut store, &gaps, &gap_bounds)
        .await
        .expect("gaps");
    assert_eq!(recorded.status, "complete");
    assert_eq!(recorded.high_water_slot, Some(50));
    assert_eq!(recorded.gaps, 3);
    assert_eq!(recorded.pending, 0);
    let outcomes = outcomes(&database_url, &gaps).await;
    assert_eq!(outcomes.get("not_found").copied(), Some(1));
    assert_eq!(outcomes.get("malformed").copied(), Some(1));
    assert_eq!(outcomes.get("unsupported_version").copied(), Some(1));
    let reason = gap_reason(&database_url, &missing).await;
    assert!(reason.contains("commitment"));
    assert!(!reason.to_ascii_lowercase().contains("never existed"));

    let repeated = run(&Script::new(vec![], vec![]), &mut store, &gaps, &gap_bounds)
        .await
        .expect("repeat");
    assert_eq!(repeated.status, "complete");
    assert_eq!(repeated.fetched, 0);
    assert_eq!(repeated.records, 3);

    let newest = signature(10);
    let middle = signature(11);
    let oldest = signature(12);
    let below = signature(13);
    let capped = window(100, 200);
    let cap_bounds = bounds(100, 200, 1);
    let cap_first = Script::new(
        vec![Page {
            before: None,
            result: Ok(vec![
                info(&newest, 180),
                info(&middle, 150),
                info(&oldest, 120),
            ]),
        }],
        vec![(newest.clone(), Ok(Some(body(180))))],
    );
    let capped_first = run(&cap_first, &mut store, &capped, &cap_bounds)
        .await
        .expect("cap");
    assert_eq!(
        capped_first.stopped_reason.as_deref(),
        Some("max_transactions")
    );
    assert!(!capped_first.enumeration_complete);
    assert_eq!(capped_first.high_water_slot, None);
    assert_eq!(capped_first.records, 1);

    let cap_second = Script::new(
        vec![Page {
            before: Some(newest.clone()),
            result: Ok(vec![
                info(&middle, 150),
                info(&oldest, 120),
                info(&below, 50),
            ]),
        }],
        vec![(middle.clone(), Ok(Some(body(150))))],
    );
    let capped_second = run(&cap_second, &mut store, &capped, &cap_bounds)
        .await
        .expect("cap resume");
    assert!(!capped_second.enumeration_complete);
    assert_eq!(capped_second.records, 2);
    assert_eq!(capped_second.high_water_slot, None);

    let cap_third = Script::new(
        vec![Page {
            before: Some(middle.clone()),
            result: Ok(vec![info(&oldest, 120), info(&below, 50)]),
        }],
        vec![(oldest.clone(), Ok(Some(body(120))))],
    );
    let capped_third = run(&cap_third, &mut store, &capped, &cap_bounds)
        .await
        .expect("cap finish");
    assert_eq!(capped_third.status, "complete");
    assert_eq!(capped_third.high_water_slot, Some(200));
    assert_eq!(capped_third.records, 3);
    assert_eq!(stored_rows(&database_url, &capped).await, 3);
}

fn signature(byte: u8) -> String {
    bs58::encode([byte; 64]).into_string()
}

fn address() -> String {
    bs58::encode([4u8; 32]).into_string()
}

fn info(signature: &str, slot: i64) -> SignatureInfo {
    SignatureInfo {
        signature: signature.to_owned(),
        slot,
        block_time: None,
    }
}

fn body(slot: i64) -> Value {
    json!({
        "slot": slot,
        "blockTime": null,
        "transaction": {
            "message": {
                "accountKeys": [address()],
                "instructions": [{"programIdIndex": 0}]
            }
        },
        "meta": {"err": null, "logMessages": ["ok"]}
    })
}

fn window(from_slot: i64, to_slot: i64) -> Window {
    Window {
        source_id: "solana-test".into(),
        cluster_id: "mainnet-beta".into(),
        address: address(),
        commitment: "finalized".into(),
        from_slot,
        to_slot,
    }
}

fn bounds(from_slot: i64, to_slot: i64, max_transactions: u32) -> Bounds {
    Bounds {
        from_slot,
        to_slot,
        max_transactions,
        concurrency: 2,
        timeout: Duration::from_secs(2),
        max_retries: 0,
        deadline: Duration::from_secs(10),
    }
}

impl Script {
    fn new(pages: Vec<Page>, transactions: Vec<(String, Result<Option<Value>, RpcError>)>) -> Self {
        Self {
            pages: Mutex::new(pages.into()),
            transactions: Mutex::new(transactions.into_iter().collect()),
            inflight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
}

async fn prepare_database(admin_url: &str) -> String {
    let (admin, connection) = tokio_postgres::connect(admin_url, NoTls)
        .await
        .expect("admin connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let exists = admin
        .query_opt(
            "SELECT 1 FROM pg_database WHERE datname = 'forwardops_ingest_test'",
            &[],
        )
        .await
        .expect("database lookup");
    if exists.is_none() {
        admin
            .batch_execute("CREATE DATABASE forwardops_ingest_test")
            .await
            .expect("create database");
    }
    let database_url = switch_database(admin_url, "forwardops_ingest_test");
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("ingest connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "DROP TABLE IF EXISTS source_records, ingestion_signatures, ingestion_checkpoints",
        )
        .await
        .expect("drop tables");
    let sql = std::fs::read_to_string(format!(
        "{}/../../migrations/004_solana_ingestion.sql",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("migration");
    client.batch_execute(&sql).await.expect("apply migration");
    database_url
}

fn switch_database(url: &str, database: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base.to_owned(), format!("?{query}")),
        None => (url.to_owned(), String::new()),
    };
    let scheme = base.find("://").map(|index| index + 3).unwrap_or(0);
    match base[scheme..].rfind('/') {
        Some(offset) => format!("{}/{database}{query}", &base[..scheme + offset]),
        None => format!("{base}/{database}{query}"),
    }
}

async fn mark_pending(database_url: &str, signature: &str) {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .execute(
            "UPDATE ingestion_signatures SET fetch_status = 'pending'
             WHERE source_id = 'solana-test' AND signature = $1 AND from_slot = 10 AND to_slot = 30",
            &[&signature],
        )
        .await
        .expect("reset signature");
    client
        .execute(
            "UPDATE ingestion_checkpoints
             SET status = 'in_progress', high_water_slot = NULL, stopped_reason = NULL
             WHERE source_id = 'solana-test' AND from_slot = 10 AND to_slot = 30",
            &[],
        )
        .await
        .expect("reset checkpoint");
}

async fn stored_rows(database_url: &str, window: &Window) -> i64 {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT count(*) FROM source_records
             WHERE source_id = $1 AND address = $2 AND commitment = $3 AND slot BETWEEN $4 AND $5",
            &[
                &window.source_id,
                &window.address,
                &window.commitment,
                &window.from_slot,
                &window.to_slot,
            ],
        )
        .await
        .expect("count");
    row.get(0)
}

async fn outcomes(database_url: &str, window: &Window) -> std::collections::BTreeMap<String, i64> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let rows = client
        .query(
            "SELECT outcome, count(*) FROM source_records
             WHERE source_id = $1 AND address = $2 AND slot BETWEEN $3 AND $4
             GROUP BY outcome",
            &[
                &window.source_id,
                &window.address,
                &window.from_slot,
                &window.to_slot,
            ],
        )
        .await
        .expect("outcomes");
    rows.into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

async fn gap_reason(database_url: &str, signature: &str) -> String {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT gap_reason FROM source_records WHERE signature = $1",
            &[&signature],
        )
        .await
        .expect("gap");
    row.get(0)
}
