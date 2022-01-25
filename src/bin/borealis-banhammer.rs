use borealis_banhammer::{
    banhammer::{self, Banhammer},
    de::RelayerInput,
};
use std::{
    fs, io,
    time::{Duration, Instant, SystemTime},
};
use tracing::{debug, info};

fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let raw_toml = fs::read_to_string("./Config.toml").expect("Missing Config.toml.");
    let ban_manager_config: banhammer::Config =
        toml::from_str(&raw_toml).expect("Failed to parse TOML.");
    let mut ban_manager = Banhammer::new(ban_manager_config);
    let time = Instant::now();
    let mut next_save = Duration::from_secs(60);

    info!(
        "{:?}",
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
    );
    info!("Starting banhammer...");
    loop {
        let mut buffer = String::new();
        let stdin = io::stdin();
        stdin.read_line(&mut buffer)?;

        let relayer_input: RelayerInput = match serde_json::from_str(&buffer) {
            Ok(r) => r,
            Err(_e) => {
                // TODO relayer failed parses
                continue;
            }
        };

        ban_manager.read_input(&relayer_input);

        ban_manager.tick(time);

        if time.elapsed() > next_save {
            debug!("Writing state");

            fs::remove_file("./clients.json").ok();
            let json = serde_json::to_string_pretty(&ban_manager.user_clients()).unwrap();
            fs::write("./clients.json", json).unwrap();

            fs::remove_file("./addresses.json").ok();
            let json = serde_json::to_string_pretty(&ban_manager.user_froms()).unwrap();
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
            let banned_froms = ban_list.froms.len();
            let banned_tokens = ban_list.tokens.len();
            info!("Banned Clients: {banned_clients}");
            info!("Banned Froms: {banned_froms}");
            info!("Banned Tokens: {banned_tokens}");
            info!("{ban_list:#?}");
            break;
        }
    }

    Ok(())
}
