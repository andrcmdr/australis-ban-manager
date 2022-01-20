use borealis_banhammer::{
    banhammer::{self, Banhammer},
    de::RelayerInput,
};
use std::{
    fs, io,
    time::{Duration, Instant},
};

fn main() -> io::Result<()> {
    let raw_toml = fs::read_to_string("./Config.toml").expect("Missing Config.toml.");
    let ban_manager_config: banhammer::Config =
        toml::from_str(&raw_toml).expect("Failed to parse TOML.");
    let mut ban_manager = Banhammer::new(ban_manager_config);
    let time = Instant::now();
    loop {
        let mut buffer = String::new();
        let stdin = io::stdin();
        stdin.read_line(&mut buffer)?;

        let relayer_input: RelayerInput = match serde_json::from_str(&buffer) {
            Ok(r) => r,
            Err(_e) => {
                continue;
            }
        };

        let ip = relayer_input.client;
        if ban_manager.ban_list().ip.contains_key(&ip) {
            continue;
        }

        let from = relayer_input.params.from;
        if ban_manager.ban_list().from.contains_key(&from) {
            continue;
        }

        if let Some(token) = relayer_input.token.clone() {
            if ban_manager.ban_list().token.contains_key(&token) {
                continue;
            }
        }

        let bans = ban_manager.read_input(&relayer_input);
        for ban in bans {
            println!("BANNED: {:?}", ban);
        }

        ban_manager.tick(time);

        if time.elapsed() > Duration::from_secs(10800) {
            let ban_list = ban_manager.ban_list();

            let banned_clients = ban_list.ip.len();
            let banned_froms = ban_list.from.len();
            let banned_tokens = ban_list.token.len();
            println!("Banned Clients: {banned_clients}");
            println!("Banned Froms: {banned_froms}");
            println!("Banned Tokens: {banned_tokens}");
            println!("{ban_list:#?}");
            break;
        }
    }

    Ok(())
}
