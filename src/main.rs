mod config;
mod index;
mod indexer;
mod server;
mod fuse;
mod zip_cache;
mod mtproxy;

use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use grammers_client::{Client, SignInError};
use grammers_mtsender::{ConnectionParams, InvocationError, SenderPool};
use grammers_session::storages::SqliteSession;
use log::{error, info, warn};
use rpassword;

use config::{Config, LogConfig};
use index::AppState;

/// Initialize the global logger.
///
/// Levels are color-coded (env_logger default palette: ERROR red, WARN yellow,
/// INFO green, DEBUG blue, TRACE cyan). Default filter is `info`; override
/// with `log:` in `tgfs.yml` or the `RUST_LOG` env var (`RUST_LOG` wins).
fn init_logger(log: Option<&LogConfig>) {
    use std::io::Write as _;
    let default_filter = log.map(|l| l.to_filter_string()).unwrap_or_else(|| "info".to_string());
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_filter))
        .format(|buf, record| {
            let ts = buf.timestamp_millis();
            let level_style = buf.default_level_style(record.level());
            writeln!(
                buf,
                "{ts} {level_style}{:5}{level_style:#} {}: {}",
                record.level(),
                record.target(),
                record.args()
            )
        })
        .init();
}

const SESSION_FILE: &str = "session.sqlite3";
const DEFAULT_CONFIG_FILE: &str = "tgfs.yml";

/// Parse `--config <path>` from CLI args. Defaults to `tgfs.yml`.
fn parse_config_path() -> String {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            return args.next().unwrap_or_else(|| {
                error!("--config requires a path argument");
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

async fn setup_proxy(config: &Config) -> anyhow::Result<Option<String>> {
    let proxy = match &config.proxy {
        Some(p) => p,
        None => return Ok(None),
    };
    match proxy.proxy_type {
        config::ProxyType::Socks5 => {
            let url = match (&proxy.user, &proxy.password) {
                (Some(u), Some(p)) => format!("socks5://{}:{}@{}:{}", u, p, proxy.host, proxy.port),
                _ => format!("socks5://{}:{}", proxy.host, proxy.port),
            };
            info!("Using SOCKS5 proxy: {}:{}", proxy.host, proxy.port);
            Ok(Some(url))
        }
        config::ProxyType::Mtproxy => {
            let secret = proxy.secret.as_deref().ok_or_else(|| {
                anyhow::anyhow!("MTProxy requires a `secret` field in the proxy config")
            })?;
            let port = mtproxy::start_bridge(&proxy.host, proxy.port, secret).await?;
            info!("Using MTProxy {}:{} via local bridge on port {port}", proxy.host, proxy.port);
            Ok(Some(format!("socks5://127.0.0.1:{port}")))
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = parse_config_path();
    let config = config::load_config(&config_path)?;
    init_logger(config.log.as_ref());
    if config.http_port.is_none() && config.mount_at.is_none() {
        return Err(anyhow::anyhow!("config must set at least one of `http_port` or `mount_at`"));
    }

    let proxy_url = setup_proxy(&config).await?;
    let mut client = make_client(config.api_id, proxy_url.clone()).await?;

    if !client.is_authorized().await? {
        info!("Sending sign-in code to {}...", config.phone);
        let token = match client.request_login_code(&config.phone, &config.api_hash).await {
            Ok(t) => t,
            Err(InvocationError::Rpc(e)) if e.is("AUTH_RESTART") => {
                warn!("Session invalidated by Telegram, resetting...");
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
                        warn!("Wrong password, try again.");
                        password_token = new_token;
                    }
                    Err(e) => return Err(e.into()),
                }
            },
            Err(e) => return Err(e.into()),
        }
        info!("Signed in successfully.");
    }

    let indexer::IndexBuildResult { mime_vec: mime_pool, channels, dir_to_channel } =
        indexer::build_index(client.clone(), &config).await?;
    let state = Arc::new(AppState {
        client,
        mime_pool,
        channels,
        dir_to_channel,
        max_fetches_per_pid: config.max_fetches_per_pid,
        fresh_docs: std::sync::Mutex::new(std::collections::HashMap::new()),
    });

    // Optionally mount FUSE filesystem in a blocking task.
    let fuse_handle = config.mount_at.as_ref().map(|mountpoint| {
        info!("Mounting FUSE filesystem at {mountpoint}");
        let fs = fuse::TgfsFS::new(Arc::clone(&state));
        let mp = mountpoint.clone();
        tokio::task::spawn_blocking(move || {
            let mut fuse_config = fuser::Config::default();
            fuse_config.acl = fuser::SessionACL::All;
            fuse_config.mount_options = vec![
                fuser::MountOption::AutoUnmount,
                fuser::MountOption::RO,
                // Match the BLKSIZE advertised in fuse.rs so the kernel issues
                // bigger reads, reducing per-call FUSE event-loop overhead.
                fuser::MountOption::CUSTOM(format!("max_read={}", fuse::BLKSIZE)),
            ];
            fuser::mount2(fs, mp, &fuse_config).expect("FUSE mount failed");
        })
    });

    // Optionally serve HTTP index.
    let http_handle = if let Some(port) = config.http_port {
        let app = server::make_router(state);
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        info!("Serving on http://{addr}");
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Some(tokio::spawn(async move {
            axum::serve(listener, app).await.expect("HTTP server failed");
        }))
    } else {
        None
    };

    // On SIGTERM or SIGINT, unmount the FUSE filesystem (if any) so the mount point
    // is clean on container restart, then exit.
    if let Some(mp) = config.mount_at.clone() {
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            let mut sigint  = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = sigint.recv()  => {}
            }
            info!("Shutting down, unmounting {mp}...");
            std::process::Command::new("fusermount").args(["-u", &mp]).status().ok();
            std::process::exit(0);
        });
    }

    // Wait for whichever services are running.
    match (fuse_handle, http_handle) {
        (Some(f), Some(h)) => { let _ = tokio::try_join!(f, h)?; }
        (Some(f), None) => { f.await?; }
        (None, Some(h)) => { h.await?; }
        (None, None) => unreachable!("validated above"),
    }

    Ok(())
}
