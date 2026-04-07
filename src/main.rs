mod index;
mod indexer;
mod server;

use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use grammers_client::{Client, SignInError};
use grammers_mtsender::{InvocationError, SenderPool};
use grammers_session::storages::SqliteSession;
use rpassword;

use index::AppState;

const AUTH_FILE: &str = "auth.json";
const SESSION_FILE: &str = "session.sqlite3";
const CONFIG_FILE: &str = "tgfs.yml";

#[derive(Serialize, Deserialize)]
struct AuthConfig {
    api_id: i32,
    api_hash: String,
    phone: String,
}

fn prompt(label: &str) -> String {
    print!("{}: ", label);
    io::stdout().flush().unwrap();
    io::stdin().lock().lines().next().unwrap().unwrap()
}

fn load_or_create_auth() -> AuthConfig {
    if std::path::Path::new(AUTH_FILE).exists() {
        let data = std::fs::read_to_string(AUTH_FILE).expect("failed to read auth file");
        if let Ok(cfg) = serde_json::from_str::<AuthConfig>(&data) {
            return cfg;
        }
        eprintln!("auth file is invalid, re-entering credentials");
    }

    println!("No auth data found. Please enter your Telegram credentials.");
    println!("Get api_id and api_hash from https://my.telegram.org/apps");

    let api_id_str = prompt("api_id");
    let api_id: i32 = api_id_str.trim().parse().expect("api_id must be a number");
    let api_hash = prompt("api_hash");
    let phone = prompt("phone (e.g. +12345678900)");

    let cfg = AuthConfig {
        api_id,
        api_hash: api_hash.trim().to_string(),
        phone: phone.trim().to_string(),
    };

    let json = serde_json::to_string_pretty(&cfg).unwrap();
    std::fs::write(AUTH_FILE, json).expect("failed to write auth file");
    println!("Credentials saved to {AUTH_FILE}");
    cfg
}

async fn make_client(api_id: i32) -> anyhow::Result<Client> {
    let session = Arc::new(SqliteSession::open(SESSION_FILE).await?);
    let pool = SenderPool::new(Arc::clone(&session), api_id);
    tokio::spawn(pool.runner.run());
    Ok(Client::new(pool.handle))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let auth = load_or_create_auth();
    let config = index::load_config(CONFIG_FILE)?;

    let mut client = make_client(auth.api_id).await?;

    if !client.is_authorized().await? {
        println!("Sending sign-in code to {}...", auth.phone);
        let token = match client.request_login_code(&auth.phone, &auth.api_hash).await {
            Ok(t) => t,
            Err(InvocationError::Rpc(e)) if e.is("AUTH_RESTART") => {
                eprintln!("Session invalidated by Telegram, resetting...");
                std::fs::remove_file(SESSION_FILE).ok();
                client = make_client(auth.api_id).await?;
                client.request_login_code(&auth.phone, &auth.api_hash).await?
            }
            Err(e) => return Err(e.into()),
        };

        let code = prompt("Enter the code you received");

        match client.sign_in(&token, code.trim()).await {
            Ok(_) => {}
            Err(SignInError::PasswordRequired(mut password_token)) => loop {
                let password = rpassword::prompt_password("2FA password: ").unwrap();
                match client.check_password(password_token, password.trim()).await {
                    Ok(_) => break,
                    Err(SignInError::InvalidPassword(new_token)) => {
                        eprintln!("Wrong password, try again.");
                        password_token = new_token;
                    }
                    Err(e) => return Err(e.into()),
                }
            },
            Err(e) => return Err(e.into()),
        }
        println!("Signed in successfully.");
    }

    let index = indexer::build_index(client.clone(), &config).await?;
    let state = Arc::new(AppState { client, index });

    let app = server::make_router(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    println!("Serving on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
