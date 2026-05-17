mod config;
mod index;
mod indexer;
mod server;
mod fuse;
mod zip_cache;
mod mtproxy;
mod realtime;

use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use grammers_client::{Client, SignInError};
use grammers_mtsender::{ConnectionParams, InvocationError, SenderPool};
use grammers_session::storages::SqliteSession;
use grammers_session::updates::UpdatesLike;
use log::{error, info, warn};
use rpassword;
use tokio::sync::mpsc;

use config::{Config, LogConfig};
use index::{AppState, MimePool};
use zip_cache::ZipCache;

/// Initialize the global logger.
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

/// Build a Client and capture the SenderPool's updates receiver so the
/// realtime dispatcher can subscribe to it. The dispatcher drives the same
/// channel that `Client::stream_updates` reads from.
async fn make_client(
    api_id: i32,
    proxy_url: Option<String>,
) -> anyhow::Result<(Client, mpsc::UnboundedReceiver<UpdatesLike>)> {
    let session = Arc::new(SqliteSession::open(SESSION_FILE).await?);
    let params = ConnectionParams { proxy_url, ..Default::default() };
    let pool = SenderPool::with_configuration(Arc::clone(&session), api_id, params);
    tokio::spawn(pool.runner.run());
    Ok((Client::new(pool.handle), pool.updates))
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
    let (mut client, mut updates_rx) = make_client(config.api_id, proxy_url.clone()).await?;

    if !client.is_authorized().await? {
        info!("Sending sign-in code to {}...", config.phone);
        let token = match client.request_login_code(&config.phone, &config.api_hash).await {
            Ok(t) => t,
            Err(InvocationError::Rpc(e)) if e.is("AUTH_RESTART") => {
                warn!("Session invalidated by Telegram, resetting...");
                std::fs::remove_file(SESSION_FILE).ok();
                let (c, rx) = make_client(config.api_id, proxy_url).await?;
                client = c;
                updates_rx = rx;
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

    let mime_pool = MimePool::new();
    let zip_cache = Arc::new(Mutex::new(ZipCache::load("zip_index_cache.json.gz")));
    let indexer::IndexBuildResult { channels, dir_to_channel } =
        indexer::build_index(client.clone(), &config, &mime_pool, &zip_cache).await?;
    // Persist the zip cache once after the full index is built; runtime
    // additions during dispatch stay in-memory only.
    if let Err(e) = zip_cache.lock().unwrap().save() {
        error!("failed to save zip cache: {}", e);
    }

    let state = Arc::new(AppState {
        client: client.clone(),
        mime_pool,
        channels,
        dir_to_channel,
        max_fetches_per_pid: config.max_fetches_per_pid,
        max_fetches_total: config.max_fetches_total,
        fresh_docs: std::sync::Mutex::new(std::collections::HashMap::new()),
    });

    // Build the FUSE handle ahead of session creation so the realtime
    // dispatcher can be wired to it. The Filesystem itself is cheaply cloned
    // (Arc<Inner>) so both fuser and the dispatcher hold their own handles.
    let fs_handle: Option<fuse::TgfsFS> = config.mount_at.as_ref().map(|_| {
        fuse::TgfsFS::new(Arc::clone(&state))
    });

    // Optionally mount FUSE filesystem in a blocking task. We use Session::new
    // instead of the convenience mount2() so we can grab a Notifier handle and
    // emit FUSE_NOTIFY_DELETE / FUSE_NOTIFY_INVAL_ENTRY events when the index
    // changes — that's how inotify watchers on the mountpoint see updates.
    let fuse_handle = if let Some(mp) = config.mount_at.clone() {
        let fs = fs_handle.as_ref().expect("fs_handle present when mount_at set").clone();
        info!("Mounting FUSE filesystem at {mp}");
        let mut fuse_config = fuser::Config::default();
        fuse_config.acl = fuser::SessionACL::All;
        fuse_config.mount_options = vec![
            fuser::MountOption::AutoUnmount,
            fuser::MountOption::RO,
            fuser::MountOption::CUSTOM(format!("max_read={}", fuse::BLKSIZE)),
        ];
        let session = fuser::Session::new(fs.clone(), &mp, &fuse_config)
            .map_err(|e| anyhow::anyhow!("FUSE session create failed: {e}"))?;
        fs.set_notifier(session.notifier());
        Some(tokio::task::spawn_blocking(move || {
            // Session::run consumes self and blocks until unmount.
            // It's pub(crate); use spawn() which runs the loop in a background
            // thread and gives us a join handle. We then block on the handle.
            match session.spawn() {
                Ok(bg) => {
                    if let Err(e) = bg.join() {
                        error!("FUSE session ended with error: {e}");
                    }
                }
                Err(e) => error!("FUSE session spawn failed: {e}"),
            }
        }))
    } else {
        None
    };

    // Spawn the realtime dispatcher. It owns its clone of the FS handle so it
    // can fire FUSE notifications when channel state mutates. When realtime is
    // off, the updates receiver is dropped so the SenderPool's unbounded
    // update buffer doesn't grow without bound.
    if config.realtime {
        let dispatcher = realtime::Dispatcher::new(
            client.clone(),
            Arc::clone(&state),
            Arc::clone(&zip_cache),
            fs_handle.clone(),
        );
        tokio::spawn(dispatcher.run(updates_rx));
    } else {
        info!("realtime updates disabled by config");
        drop(updates_rx);
    }

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

    // On SIGTERM or SIGINT, unmount the FUSE filesystem (if any).
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

    match (fuse_handle, http_handle) {
        (Some(f), Some(h)) => { let _ = tokio::try_join!(f, h)?; }
        (Some(f), None) => { f.await?; }
        (None, Some(h)) => { h.await?; }
        (None, None) => unreachable!("validated above"),
    }

    Ok(())
}
