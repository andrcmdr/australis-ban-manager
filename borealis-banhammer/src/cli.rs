use clap::Parser;

use tracing_subscriber::EnvFilter;

use core::str::FromStr;
use std::string::ToString;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

/// CLI options (subcommands and flags)
#[derive(Parser, Debug, Clone)]
#[clap(version = "0.1.0", author = "Aurora <hello@aurora.dev>")]
#[clap(subcommand_required = true)]
#[clap(arg_required_else_help = true)]
pub(crate) struct Opts {
    /// Verbosity level for extensive output to stdout or log
    #[clap(short, long)]
    pub verbose: Option<VerbosityLevel>,
    /// Custom directory for configurations and state. Defaults to ./.borealis-banhammer/
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
    Check(Context),
    /// Run Borealis Banhammer with options
    Run(Context),
}

/// CLI options to run Borealis Banhammer
#[derive(Parser, Debug, Clone)]
pub(crate) struct Context {
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
        default_value = "tls://europe.nats.backend.aurora.dev:4222,tls://eastcoast.nats.backend.aurora.dev:4222,tls://westcoast.nats.backend.aurora.dev:4222"
    )]
    pub nats_server: String,
    /// Receive relayer (nginx) interceptor messages from subject
    /// ("relayer.rpc.sendrawtx" or "testnet.relayer.rpc.sendrawtx")
    #[clap(long, default_value = "relayer.rpc.sendrawtx")]
    pub relayer_rx_subject: String,
    /// Receive Banhammer buckets configuration messages from subject
    #[clap(long, default_value = "banhammer.config.messages")]
    pub config_rx_subject: String,
    /// Receive eth_call messages from subject
    #[clap(long, default_value = "eth_call.messages")]
    pub eth_call_rx_subject: String,
    /// Stream banning event messages to subject
    #[clap(long, default_value = "banhammer.ban.messages")]
    pub tx_subject: String,
}

/// Verbosity level for messages dump to log and stdout:
/// WithRuntimeThreadsDump - full dump of run-time Tokio reactor activity and threads state in a thread pool
/// (threads start/stop/park/unpark) for run-time debugging
/// WithBanhammerMessagesDump - full dump of Banhammer rx and tx messages received/sent from/to corresponding subjects
#[derive(Parser, Debug, Clone, Copy, Eq, PartialEq)]
pub enum VerbosityLevel {
    WithRuntimeThreadsDump,
    WithBanhammerMessagesDump,
}

impl FromStr for VerbosityLevel {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let input = s.to_lowercase();
        match input.as_str() {
            "0" | "withruntimethreadsdump" => Ok(VerbosityLevel::WithRuntimeThreadsDump),
            "1" | "withbanhammermessagesdump" => Ok(VerbosityLevel::WithBanhammerMessagesDump),
            _ => Err("Unknown output verbosity level: `--verbose` should be `WithRuntimeThreadsDump` (`0`) or `WithBanhammerMessagesDump` (`1`)".to_string().into()),
        }
    }
}

/// Initialize logging
pub(crate) fn init_logging() {
    // Filters can be customized through RUST_LOG environment variable via CLI
    let mut env_filter = EnvFilter::new(
        "borealis_banhammer=info,borealis_banhammer_run_time=info,borealis_banhammer_nats=info,borealis_banhammer_buckets=info",
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
