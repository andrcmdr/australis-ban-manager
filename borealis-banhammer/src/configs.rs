use clap::Parser;

use near_indexer::near_primitives::types::Gas;

use tracing_subscriber::EnvFilter;

use core::str::FromStr;
use std::string::ToString;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

/// CLI options (subcommands and flags)
#[derive(Parser, Debug, Clone)]
#[clap(version = "0.10.0", author = "Aurora <hello@aurora.dev>")]
#[clap(subcommand_required = true)]
#[clap(arg_required_else_help = true)]
pub(crate) struct Opts {
    /// Verbosity level for extensive output to stdout or log
    #[clap(short, long)]
    pub verbose: Option<VerbosityLevel>,
    /// Custom directory for configurations and state. Defaults to ./.borealis-indexer/
    #[clap(short, long)]
    pub home_dir: Option<std::path::PathBuf>,
    //  /// Configuration file path
    //  #[clap(short, long)]
    //  pub config_path: Option<std::path::PathBuf>,
    /// Subcommands
    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

/// CLI subcommands
#[derive(Parser, Debug, Clone)]
pub(crate) enum SubCommand {
    /// Checking connection to NATS
    Check(RunArgs),
    /// Initialize Borealis Indexer configuration
    Init(InitConfigArgs),
    /// Run Borealis Indexer with options and sync blocks from the network
    Run(RunArgs),
}

/// CLI options to run Borealis Indexer
#[derive(Parser, Debug, Clone)]
pub(crate) struct RunArgs {
    /// root CA certificate
    #[clap(long)]
    pub root_cert_path: Option<std::path::PathBuf>,
    /// client certificate
    #[clap(long)]
    pub client_cert_path: Option<std::path::PathBuf>,
    /// client private key
    #[clap(long)]
    pub client_private_key: Option<std::path::PathBuf>,
    /// Path to NATS credentials (JWT/NKEY tokens)
    #[clap(short, long)]
    pub creds_path: Option<std::path::PathBuf>,
    /// Borealis Bus (NATS based MOM/MQ/SOA service bus) protocol://address:port
    /// Example: "nats://borealis.aurora.dev:4222" or "tls://borealis.aurora.dev:4443" for TLS connection
    #[clap(
        long,
        default_value = "tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222"
    )]
    pub nats_server: String,
    /// Stream messages to subject
    #[clap(long, default_value = "BlockIndex_StreamerMessages")]
    pub subject: String,
    /// Streaming messages format (`CBOR` or `JSON`), suffix for subject name
    #[clap(long, default_value = "CBOR")]
    pub msg_format: MsgFormat,
    /// Compress the payload of Borealis Message
    #[clap(long)]
    pub payload_compression: Option<CompressionMode>,
    #[clap(long, default_value = "FromInterruption")]
    pub sync_mode: SyncMode,
    #[clap(long)]
    pub block_height: Option<u64>,
    #[clap(long, default_value = "StreamWhileSyncing")]
    pub await_synced: AwaitSynced,
}

/// Streaming messages format (should be upper case, 'cause it's a suffix for `subject` name, and NATS subject is case sensitive)
#[derive(Parser, Debug, Clone, Copy)]
pub(crate) enum MsgFormat {
    Cbor,
    Json,
}

impl FromStr for MsgFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.to_lowercase();
        match input.as_str() {
            "cbor" => Ok(MsgFormat::Cbor),
            "json" => Ok(MsgFormat::Json),
            _ => Err(
                "Unknown message format: `--msg-fomat` should contain `CBOR` or `JSON`"
                    .to_string()
                    .into(),
            ),
        }
    }
}

impl ToString for MsgFormat {
    fn to_string(&self) -> String {
        match self {
            MsgFormat::Cbor => String::from("CBOR"),
            MsgFormat::Json => String::from("JSON"),
        }
    }
}

/// Compression mode for NATS Message payload
#[derive(Parser, Debug, Clone, Copy)]
pub(crate) enum CompressionMode {
    Lz4f,
    Zstd,
}

impl FromStr for CompressionMode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.to_lowercase();
        match input.as_str() {
            "lz4f" => Ok(CompressionMode::Lz4f),
            "zstd" => Ok(CompressionMode::Zstd),
            _ => Err(
                "Unknown payload compression mode: `--payload-compression` should be `LZ4F` or `Zstd`"
                    .to_string()
                    .into(),
            ),
        }
    }
}

/// Definition of a syncing mode for NEAR Indexer
#[derive(Parser, Debug, Clone, Copy)]
pub(crate) enum SyncMode {
    /// Real-time syncing, always taking the latest finalized block to stream
    LatestSynced,
    /// Starts syncing from the block NEAR Indexer was interrupted last time
    FromInterruption,
    /// Specific block height to start syncing from, RunArgs.block_height should follow after it
    BlockHeight,
}

impl FromStr for SyncMode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.to_lowercase();
        match input.as_str() {
            "latestsynced" => Ok(SyncMode::LatestSynced),
            "frominterruption" => Ok(SyncMode::FromInterruption),
            "blockheight" => Ok(SyncMode::BlockHeight),
            _ => Err("Unknown indexer synchronization mode: `--sync-mode` should be `LatestSynced`, `FromInterruption` or `BlockHeight` with --block-height explicit pointing".to_string().into()),
        }
    }
}

/// Define whether await for node to be fully synced or stream while syncing (useful for indexing from genesis)
#[derive(Parser, Debug, Clone, Copy)]
pub(crate) enum AwaitSynced {
    /// Don't stream until the node is fully synced
    WaitForFullSync,
    /// Stream while node is syncing
    StreamWhileSyncing,
}

impl FromStr for AwaitSynced {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.to_lowercase();
        match input.as_str() {
            "waitforfullsync" => Ok(AwaitSynced::WaitForFullSync),
            "streamwhilesyncing" => Ok(AwaitSynced::StreamWhileSyncing),
            _ => Err("Unknown indexer node await synchronization mode: `--await-synced` should be `WaitForFullSync` or `StreamWhileSyncing`".to_string().into()),
        }
    }
}

/// Verbosity level for messages dump to log and stdout:
/// WithBlockHashHeight - output only block height & hash
/// WithStreamerMessageDump - full dump of `StreamerMessage`
/// WithStreamerMessageParse - full dump with full parse of `StreamerMessage`
#[derive(Parser, Debug, Clone, Copy, Eq, PartialEq)]
pub enum VerbosityLevel {
    WithRuntimeThreadsDump,
    WithStreamerMessageDump,
    WithStreamerMessageParse,
}

impl FromStr for VerbosityLevel {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.to_lowercase();
        match input.as_str() {
            "0" | "withruntimethreadsdump" => Ok(VerbosityLevel::WithRuntimeThreadsDump),
            "1" | "withstreamermessagedump" => Ok(VerbosityLevel::WithStreamerMessageDump),
            "2" | "withstreamermessageparse" => Ok(VerbosityLevel::WithStreamerMessageParse),
            _ => Err("Unknown output verbosity level: `--verbose` should be `WithBlockHashHeight` (`0`), `WithStreamerMessageDump` (`1`) or `WithStreamerMessageParse` (`2`)".to_string().into()),
        }
    }
}

/// Override standard config args with CLI options
#[derive(Parser, Debug, Clone)]
pub(crate) struct InitConfigArgs {
    /// chain/network id (localnet, devnet, testnet, betanet, mainnet)
    #[clap(short, long)]
    pub chain_id: Option<String>,
    /// Account ID for the validator key
    #[clap(long)]
    pub account_id: Option<String>,
    /// Specify private key generated from seed (TESTING ONLY)
    #[clap(long)]
    pub test_seed: Option<String>,
    /// Number of shards to initialize the chain with
    #[clap(short, long, default_value = "1")]
    pub num_shards: u64,
    /// Makes block production fast (TESTING ONLY)
    #[clap(short, long)]
    pub fast: bool,
    /// Genesis file to use when initialize testnet (including downloading)
    #[clap(short, long)]
    pub genesis: Option<String>,
    /// Download the verified NEAR genesis file automatically.
    #[clap(long)]
    pub download_genesis: bool,
    /// Specify a custom download URL for the genesis-file.
    #[clap(long)]
    pub download_genesis_url: Option<String>,
    /// Download the verified NEAR config file automatically.
    #[clap(long)]
    pub download_config: bool,
    /// Specify a custom download URL for the config file.
    #[clap(long)]
    pub download_config_url: Option<String>,
    /// Specify the boot nodes to bootstrap the network
    #[clap(long)]
    pub boot_nodes: Option<String>,
    /// Specify a custom max_gas_burnt_view limit.
    #[clap(long)]
    pub max_gas_burnt_view: Option<Gas>,
}

/// Override standard config args with CLI options
impl From<InitConfigArgs> for near_indexer::InitConfigArgs {
    fn from(config_args: InitConfigArgs) -> Self {
        Self {
            chain_id: config_args.chain_id,
            account_id: config_args.account_id,
            test_seed: config_args.test_seed,
            num_shards: config_args.num_shards,
            fast: config_args.fast,
            genesis: config_args.genesis,
            download_genesis: config_args.download_genesis,
            download_genesis_url: config_args.download_genesis_url,
            download_config: config_args.download_config,
            download_config_url: config_args.download_config_url,
            boot_nodes: config_args.boot_nodes,
            max_gas_burnt_view: config_args.max_gas_burnt_view,
        }
    }
}

/// Initialize logging
pub(crate) fn init_logging() {

    // Filters can be customized through RUST_LOG environment variable via CLI
    let mut env_filter = EnvFilter::new(
        "tokio_reactor=info,near=info,near=error,stats=info,telemetry=info,near-performance-metrics=info,aggregated=info,near_indexer=info,borealis_indexer=info,borealis_consumer=info",
    );

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stdout)
        .init();

}
