use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use log::{error, info};

use tgfs::{config, fuse, indexer, realtime, server};
use tgfs::config::LogConfig;
use tgfs::index::{AppState, MimePool};
use tgfs::login::connect_and_authorize;
use tgfs::zip_cache::ZipCache;

/// Initialize the global logger.
fn init_logger(log: Option<&LogConfig>) {
    use std::io::Write as _;
    let default_filter = log
        .and_then(|l| l.level.as_ref())
        .map(|l| l.to_filter_string())
        .unwrap_or_else(|| "info".to_string());
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = parse_config_path();
    let config = config::load_config(&config_path)?;
    init_logger(config.log.as_ref());
    if config.http_port.is_none() && config.mount_at.is_none() {
        return Err(anyhow::anyhow!("config must set at least one of `http_port` or `mount_at`"));
    }

    let (client, updates_rx) = connect_and_authorize(&config).await?;

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
