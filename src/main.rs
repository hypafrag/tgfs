mod index;
mod indexer;
mod server;
mod fuse;

use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use grammers_client::{Client, SignInError};
use grammers_mtsender::{ConnectionParams, InvocationError, SenderPool};
use grammers_session::storages::SqliteSession;
use rpassword;

use index::AppState;

const SESSION_FILE: &str = "session.sqlite3";
const DEFAULT_CONFIG_FILE: &str = "tgfs.yml";

/// Parse `--config <path>` from CLI args. Defaults to `tgfs.yml`.
fn parse_config_path() -> String {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            return args.next().unwrap_or_else(|| {
                eprintln!("--config requires a path argument");
                std::process::exit(2);
            });
        }
    }
    DEFAULT_CONFIG_FILE.to_string()
}

fn prompt(label: &str) -> String {
    print!("{}: ", label);
    io::stdout().flush().unwrap();
    io::stdin().lock().lines().next().unwrap().unwrap().trim().to_string()
}

async fn make_client(api_id: i32, proxy_url: Option<String>) -> anyhow::Result<Client> {
    let session = Arc::new(SqliteSession::open(SESSION_FILE).await?);
    let params = ConnectionParams { proxy_url, ..Default::default() };
    let pool = SenderPool::with_configuration(Arc::clone(&session), api_id, params);
    tokio::spawn(pool.runner.run());
    Ok(Client::new(pool.handle))
}

fn build_proxy_url(config: &index::Config) -> Option<String> {
    let proxy = config.proxy.as_ref()?;
    match (&proxy.user, &proxy.password) {
        (Some(user), Some(pass)) => Some(format!("socks5://{}:{}@{}:{}", user, pass, proxy.host, proxy.port)),
        _ => Some(format!("socks5://{}:{}", proxy.host, proxy.port)),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = parse_config_path();
    let config = index::load_config(&config_path)?;
    if config.http_port.is_none() && config.mount_at.is_none() {
        return Err(anyhow::anyhow!("config must set at least one of `http_port` or `mount_at`"));
    }

    let proxy_url = build_proxy_url(&config);
    if let Some(ref url) = proxy_url {
        println!("Using proxy: {}", url);
    }
    let mut client = make_client(config.api_id, proxy_url.clone()).await?;

    if !client.is_authorized().await? {
        println!("Sending sign-in code to {}...", config.phone);
        let token = match client.request_login_code(&config.phone, &config.api_hash).await {
            Ok(t) => t,
            Err(InvocationError::Rpc(e)) if e.is("AUTH_RESTART") => {
                eprintln!("Session invalidated by Telegram, resetting...");
                std::fs::remove_file(SESSION_FILE).ok();
                client = make_client(config.api_id, proxy_url).await?;
                client.request_login_code(&config.phone, &config.api_hash).await?
            }
            Err(e) => return Err(e.into()),
        };

        let code = prompt("Enter the code you received");

        match client.sign_in(&token, &code).await {
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

    let indexer::IndexBuildResult { index, mime_vec: mime_pool, channel_view_map: channel_archive_view, dir_to_channel } =
        indexer::build_index(client.clone(), &config).await?;
    let state = Arc::new(AppState { client, index, mime_pool, channel_archive_view, dir_to_channel });

    // Optionally mount FUSE filesystem in a blocking task.
    let fuse_handle = config.mount_at.as_ref().map(|mountpoint| {
        println!("Mounting FUSE filesystem at {mountpoint}");
        let fs = fuse::TgfsFS::new(Arc::clone(&state));
        let mp = mountpoint.clone();
        tokio::task::spawn_blocking(move || {
            fuser::mount2(fs, mp, &[fuser::MountOption::AllowOther, fuser::MountOption::AutoUnmount]).expect("FUSE mount failed");
        })
    });

    // Optionally serve HTTP index.
    let http_handle = if let Some(port) = config.http_port {
        let app = server::make_router(state);
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        println!("Serving on http://{addr}");
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Some(tokio::spawn(async move {
            axum::serve(listener, app).await.expect("HTTP server failed");
        }))
    } else {
        None
    };

    // Wait for whichever services are running.
    match (fuse_handle, http_handle) {
        (Some(f), Some(h)) => { let _ = tokio::try_join!(f, h)?; }
        (Some(f), None) => { f.await?; }
        (None, Some(h)) => { h.await?; }
        (None, None) => unreachable!("validated above"),
    }

    Ok(())
}
