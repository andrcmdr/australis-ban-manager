use borealis_banhammer::{
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
use std::{
    fs, io,
    time::{Duration, Instant},
};
use tokio::join;
use tracing::{debug, error, info};

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

async fn serve() {
    // TODO: set ad config param
    let addr = ([127, 0, 0, 1], 9898).into();
    info!("Listening on http://{}", addr);

    let serve_future = Server::bind(&addr).serve(make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(serve_req))
    }));

    if let Err(err) = serve_future.await {
        error!("server error: {}", err);
    }
}

async fn process(ban_manager_config: banhammer::Config) {
    let mut ban_manager = Banhammer::new(ban_manager_config);
    let time = Instant::now();
    let mut next_save = Duration::from_secs(60);

    info!("Starting banhammer...");
    loop {
        let mut buffer = String::new();
        let stdin = io::stdin();
        stdin.read_line(&mut buffer).expect("failed read input");

        let relayer_input: RelayerMessage = match serde_json::from_str(&buffer) {
            Ok(r) => r,
            Err(_e) => {
                // TODO relayer failed parses
                error!("failed to parse: {}", buffer);
                continue;
            }
        };

        ban_manager.read_input(&relayer_input);
        Measure::inc(Counter::MessagesProcessed);

        ban_manager.tick(time);

        if time.elapsed() > next_save {
            debug!("Writing state");

            fs::remove_file("./clients.json").ok();
            let json = serde_json::to_string_pretty(&ban_manager.user_clients()).unwrap();
            fs::write("./clients.json", json).unwrap();

            fs::remove_file("./addresses.json").ok();
            let json = serde_json::to_string_pretty(&ban_manager.user_addresses()).unwrap();
            fs::write("./addresses.json", json).unwrap();

            fs::remove_file("./tokens.json").ok();
            let json = serde_json::to_string_pretty(&ban_manager.user_tokens()).unwrap();
            fs::write("./tokens.json", json).unwrap();

            fs::remove_file("./bans.json").ok();
            let json = serde_json::to_string_pretty(&ban_manager.bans()).unwrap();
            fs::write("./bans.json", json).unwrap();
            next_save += Duration::from_secs(60);
        }

        if time.elapsed() > Duration::from_secs(10800) {
            let ban_list = ban_manager.bans();

            let banned_clients = ban_list.clients.len();
            let banned_addresses = ban_list.addresses.len();
            let banned_tokens = ban_list.tokens.len();
            info!("Banned Clients: {banned_clients}");
            info!("Banned Addresses: {banned_addresses}");
            info!("Banned Tokens: {banned_tokens}");
            break;
        }
    }
}

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
