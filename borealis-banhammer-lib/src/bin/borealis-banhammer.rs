use borealis_banhammer_lib::{
    banhammer::{self, Banhammer},
    de::RelayerMessage,
    stats::{Counter, Measure},
};
use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use prometheus::Encoder;
use std::{fs, io, time::Instant};
use tokio::join;
use tracing::{debug, error, info};

/// Serve requests for Prometheus measure response
async fn serve_req(_req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let buffer = Measure::gather();
    let encoder = prometheus::TextEncoder::new();
    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();
    Ok(response)
}

/// Hyper HTTP listener
async fn serve() {
    let addr = ([127, 0, 0, 1], 9898).into();
    info!("Listening on http://{}", addr);

    let serve_future = Server::bind(&addr).serve(make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(serve_req))
    }));

    if let Err(err) = serve_future.await {
        error!("server error: {}", err);
    }
}

/// Process leaky buckets ban
async fn process(ban_manager_config: banhammer::Config) {
    let mut ban_manager = Banhammer::new(ban_manager_config);
    let time = Instant::now();

    info!("Starting banhammer...");
    loop {
        // Read input data from STDIN
        let mut buffer = String::new();
        let stdin = io::stdin();
        stdin.read_line(&mut buffer).expect("failed read input");

        // Parse relayer message
        let relayer_input: RelayerMessage = match serde_json::from_str(&buffer) {
            Ok(r) => r,
            Err(err) => {
                error!("failed to parse: {}\nwith errror: {:>}", buffer, err);
                continue;
            }
        };

        // Read raleyer message and process leaky buckets.
        // As result - Ban Events
        let ban_events = ban_manager.read_input(&relayer_input);
        debug!("ban events count: {}", ban_events.len());
        Measure::inc(Counter::MessagesReceived);
        for ban_event in ban_events {
            info!("Ban event: {:?}", ban_event);
            Measure::inc(Counter::MessagesSent);
            Measure::inc(Counter::BanReason(ban_event.clone()));
        }
        Measure::inc(Counter::MessagesProcessed);

        ban_manager.tick(time);
    }
}

/// Handle all asyc tasks
async fn handle(ban_manager_config: banhammer::Config) {
    join!(serve(), process(ban_manager_config));
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let raw_toml = fs::read_to_string("./Config.toml").expect("Missing Config.toml.");
    let ban_manager_config: banhammer::Config =
        toml::from_str(&raw_toml).expect("Failed to parse TOML.");

    handle(ban_manager_config).await;
}
