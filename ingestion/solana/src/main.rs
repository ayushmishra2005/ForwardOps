use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;

use forwardops_solana_ingest::config::{database_url, load_source, validate_bounds, Bounds};
use forwardops_solana_ingest::ingest::run;
use forwardops_solana_ingest::rpc::RpcClient;
use forwardops_solana_ingest::storage::{Store, Window};

#[derive(Parser)]
#[command(
    name = "forwardops-solana-ingest",
    about = "Bounded read-only Solana backfill"
)]
struct Cli {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    source: String,
    #[arg(long)]
    address: String,
    #[arg(long)]
    from_slot: i64,
    #[arg(long)]
    to_slot: i64,
    #[arg(long)]
    max_transactions: u32,
    #[arg(long, default_value_t = 4)]
    concurrency: usize,
    #[arg(long, default_value_t = 8)]
    timeout_seconds: u64,
    #[arg(long, default_value_t = 3)]
    max_retries: u32,
    #[arg(long, default_value_t = 120)]
    deadline_seconds: u64,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match runtime().block_on(execute(cli)) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("ingest failed: {error}");
            ExitCode::from(1)
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

async fn execute(cli: Cli) -> Result<ExitCode, forwardops_solana_ingest::IngestError> {
    let bounds = Bounds {
        from_slot: cli.from_slot,
        to_slot: cli.to_slot,
        max_transactions: cli.max_transactions,
        concurrency: cli.concurrency,
        timeout: Duration::from_secs(cli.timeout_seconds),
        max_retries: cli.max_retries,
        deadline: Duration::from_secs(cli.deadline_seconds),
    };
    validate_bounds(&bounds)?;
    let source = load_source(&cli.config, &cli.source, &cli.address)?;
    let mut store = Store::connect(&database_url()?).await?;
    let rpc = RpcClient::new(&source, bounds.timeout)?;
    let window = Window {
        source_id: source.id.clone(),
        cluster_id: source.cluster.clone(),
        address: cli.address,
        commitment: source.commitment.clone(),
        from_slot: bounds.from_slot,
        to_slot: bounds.to_slot,
    };
    let report = run(&rpc, &mut store, &window, &bounds).await?;
    println!(
        "status={} source={} stored={} gaps={} pending={} high_water_slot={} retries={} fetched={} enumeration_complete={} stopped_reason={}",
        report.status,
        window.source_id,
        report.stored,
        report.gaps,
        report.pending,
        report.high_water_slot.map(|slot| slot.to_string()).unwrap_or_else(|| "none".into()),
        report.retries,
        report.fetched,
        report.enumeration_complete,
        report.stopped_reason.unwrap_or_else(|| "none".into())
    );
    Ok(if report.status == "complete" {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(2)
    })
}
