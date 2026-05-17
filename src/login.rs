//! Shared Telegram connect + sign-in plumbing used by both the production
//! binary (`src/main.rs`) and the integration-test example
//! (`examples/integration_test.rs`). Extracted so the two paths don't drift
//! out of sync on auth handling.

use std::io::{self, BufRead, Write};
use std::sync::Arc;

use grammers_client::{Client, SignInError};
use grammers_mtsender::{ConnectionParams, InvocationError, SenderPool};
use grammers_session::storages::SqliteSession;
use grammers_session::updates::UpdatesLike;
use log::{info, warn};
use tokio::sync::mpsc;

use crate::config::{Config, ProxyType};
use crate::mtproxy;

pub const SESSION_FILE: &str = "session.sqlite3";

fn prompt(label: &str) -> String {
    print!("{}: ", label);
    io::stdout().flush().unwrap();
    io::stdin().lock().lines().next().unwrap().unwrap().trim().to_string()
}

/// Build a Client and capture the SenderPool's updates receiver so the
/// realtime dispatcher can subscribe to it.
pub async fn make_client(
    api_id: i32,
    proxy_url: Option<String>,
) -> anyhow::Result<(Client, mpsc::UnboundedReceiver<UpdatesLike>)> {
    make_client_with_session(api_id, proxy_url, SESSION_FILE).await
}

/// Same as [`make_client`] but reads the SQLite session from `session_path`
/// instead of the default `session.sqlite3` in the current directory.
pub async fn make_client_with_session(
    api_id: i32,
    proxy_url: Option<String>,
    session_path: &str,
) -> anyhow::Result<(Client, mpsc::UnboundedReceiver<UpdatesLike>)> {
    let session = Arc::new(SqliteSession::open(session_path).await?);
    let params = ConnectionParams { proxy_url, ..Default::default() };
    let pool = SenderPool::with_configuration(Arc::clone(&session), api_id, params);
    tokio::spawn(pool.runner.run());
    Ok((Client::new(pool.handle), pool.updates))
}

/// Resolve the proxy URL (if any) from a config. Starts the MTProxy bridge
/// when needed.
pub async fn setup_proxy(config: &Config) -> anyhow::Result<Option<String>> {
    let proxy = match &config.proxy {
        Some(p) => p,
        None => return Ok(None),
    };
    match proxy.proxy_type {
        ProxyType::Socks5 => {
            let url = match (&proxy.user, &proxy.password) {
                (Some(u), Some(p)) => format!("socks5://{}:{}@{}:{}", u, p, proxy.host, proxy.port),
                _ => format!("socks5://{}:{}", proxy.host, proxy.port),
            };
            info!("Using SOCKS5 proxy: {}:{}", proxy.host, proxy.port);
            Ok(Some(url))
        }
        ProxyType::Mtproxy => {
            let secret = proxy.secret.as_deref().ok_or_else(|| {
                anyhow::anyhow!("MTProxy requires a `secret` field in the proxy config")
            })?;
            let port = mtproxy::start_bridge(&proxy.host, proxy.port, secret).await?;
            info!("Using MTProxy {}:{} via local bridge on port {port}", proxy.host, proxy.port);
            Ok(Some(format!("socks5://127.0.0.1:{port}")))
        }
    }
}

/// Connect to Telegram and complete sign-in if needed. Returns an authorized
/// `Client` plus the updates receiver. Prompts on stdin for the SMS code and
/// 2FA password when required.
pub async fn connect_and_authorize(
    config: &Config,
) -> anyhow::Result<(Client, mpsc::UnboundedReceiver<UpdatesLike>)> {
    connect_and_authorize_with_session(config, SESSION_FILE).await
}

/// Like [`connect_and_authorize`] but stores/reads the SQLite session at
/// `session_path`. Used by `tgup` to keep the session under
/// `~/.config/tgfs/session.sqlite3` rather than the current directory.
pub async fn connect_and_authorize_with_session(
    config: &Config,
    session_path: &str,
) -> anyhow::Result<(Client, mpsc::UnboundedReceiver<UpdatesLike>)> {
    let proxy_url = setup_proxy(config).await?;
    let (mut client, mut updates_rx) =
        make_client_with_session(config.api_id, proxy_url.clone(), session_path).await?;

    if client.is_authorized().await? {
        return Ok((client, updates_rx));
    }

    info!("Sending sign-in code to {}...", config.phone);
    let token = match client.request_login_code(&config.phone, &config.api_hash).await {
        Ok(t) => t,
        Err(InvocationError::Rpc(e)) if e.is("AUTH_RESTART") => {
            warn!("Session invalidated by Telegram, resetting...");
            std::fs::remove_file(session_path).ok();
            let (c, rx) =
                make_client_with_session(config.api_id, proxy_url, session_path).await?;
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
    Ok((client, updates_rx))
}
