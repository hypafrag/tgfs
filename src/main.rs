use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, StatusCode};
use axum::response::{Html, Response};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use grammers_client::media::{Document, Media};
use grammers_client::peer::Peer;
use grammers_client::{Client, SignInError};
use grammers_mtsender::{InvocationError, SenderPool};
use grammers_session::storages::SqliteSession;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::ReceiverStream;

const AUTH_FILE: &str = "auth.json";
const SESSION_FILE: &str = "session.sqlite3";
const CONFIG_FILE: &str = "tgfs.yml";

#[derive(Serialize, Deserialize)]
struct AuthConfig {
    api_id: i32,
    api_hash: String,
    phone: String,
}

#[derive(Deserialize)]
struct ChannelEntry {
    name: String,
}

#[derive(Deserialize)]
struct Config {
    #[serde(default = "default_port")]
    port: u16,
    channels: Vec<ChannelEntry>,
}

fn default_port() -> u16 {
    8080
}

struct FileEntry {
    name: String,
    doc: Document,
    size: Option<usize>,
    mime_type: String,
}

struct AppState {
    client: Client,
    index: HashMap<String, Vec<FileEntry>>,
}

fn prompt(label: &str) -> String {
    print!("{}: ", label);
    io::stdout().flush().unwrap();
    io::stdin().lock().lines().next().unwrap().unwrap()
}

fn load_or_create_auth() -> AuthConfig {
    if Path::new(AUTH_FILE).exists() {
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

fn load_config() -> anyhow::Result<Config> {
    let data = std::fs::read_to_string(CONFIG_FILE)
        .map_err(|_| anyhow::anyhow!("{CONFIG_FILE} not found"))?;
    Ok(serde_yaml::from_str(&data)?)
}

async fn make_client(api_id: i32) -> anyhow::Result<Client> {
    let session = Arc::new(SqliteSession::open(SESSION_FILE).await?);
    let pool = SenderPool::new(Arc::clone(&session), api_id);
    tokio::spawn(pool.runner.run());
    Ok(Client::new(pool.handle))
}

fn resolve_filename(msg: &grammers_client::message::Message, doc: &Document) -> String {
    if msg.grouped_id().is_none() {
        for line in msg.text().lines() {
            if let Some(value) = line.strip_prefix("name:") {
                return value.trim().to_string();
            }
        }
    }
    doc.name().unwrap_or("<unnamed>").to_string()
}

fn human_size(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "K", "M", "G", "T"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}B")
    } else {
        format!("{:.1}{}", value, UNITS[unit])
    }
}

struct Entry {
    href: String,
    label: String,
    size: Option<usize>,
}

fn dir_listing(title: &str, parent: Option<&str>, entries: &[Entry]) -> String {
    let mut body = format!(
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 3.2 Final//EN\">\n\
         <html>\n<head><title>{title}</title></head>\n<body>\n\
         <h1>{title}</h1>\n<pre>\n"
    );
    if let Some(p) = parent {
        body.push_str(&format!("<a href=\"{p}\">../</a>\n"));
    }
    for e in entries {
        let size_str = e.size.map_or("-".to_string(), human_size);
        body.push_str(&format!("<a href=\"{}\">{}</a>  {size_str}\n", e.href, e.label));
    }
    body.push_str("</pre>\n</body>\n</html>");
    body
}

async fn handle_root(State(state): State<Arc<AppState>>) -> Html<String> {
    let mut channels: Vec<&String> = state.index.keys().collect();
    channels.sort();
    let entries: Vec<Entry> = channels
        .iter()
        .map(|k| Entry {
            href: format!("{}/", urlencoding::encode(k)),
            label: format!("{k}/"),
            size: None,
        })
        .collect();
    Html(dir_listing("Index of /", None, &entries))
}

async fn handle_channel(
    State(state): State<Arc<AppState>>,
    AxumPath(channel): AxumPath<String>,
) -> Result<Html<String>, StatusCode> {
    let files = state.index.get(&channel).ok_or(StatusCode::NOT_FOUND)?;
    let entries: Vec<Entry> = files
        .iter()
        .map(|f| Entry {
            href: urlencoding::encode(&f.name).into_owned(),
            label: f.name.clone(),
            size: f.size,
        })
        .collect();
    let title = format!("Index of /{channel}/");
    Ok(Html(dir_listing(&title, Some("/"), &entries)))
}

async fn handle_download(
    State(state): State<Arc<AppState>>,
    AxumPath((channel, filename)): AxumPath<(String, String)>,
) -> Result<Response, StatusCode> {
    let files = state.index.get(&channel).ok_or(StatusCode::NOT_FOUND)?;
    let entry = files
        .iter()
        .find(|e| e.name == filename)
        .ok_or(StatusCode::NOT_FOUND)?;

    let client = state.client.clone();
    let doc = entry.doc.clone();
    let mime = entry.mime_type.clone();
    let size = entry.size;
    let encoded_name = urlencoding::encode(&entry.name).into_owned();

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        let mut dl = client.iter_download(&doc);
        while let Ok(Some(chunk)) = dl.next().await {
            if tx
                .send(Ok::<Bytes, std::io::Error>(Bytes::from(chunk)))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let body = Body::from_stream(ReceiverStream::new(rx));
    let content_disposition = format!("attachment; filename*=UTF-8''{encoded_name}");

    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, mime)
        .header(header::CONTENT_DISPOSITION, content_disposition);
    if let Some(s) = size {
        builder = builder.header(header::CONTENT_LENGTH, s);
    }
    Ok(builder.body(body).unwrap())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let auth = load_or_create_auth();
    let config = load_config()?;

    let mut client = make_client(auth.api_id).await?;

    if !client.is_authorized().await? {
        println!("Sending sign-in code to {}...", auth.phone);
        let token = match client.request_login_code(&auth.phone, &auth.api_hash).await {
            Ok(t) => t,
            Err(InvocationError::Rpc(e)) if e.is("AUTH_RESTART") => {
                eprintln!("Session invalidated by Telegram, resetting...");
                std::fs::remove_file(SESSION_FILE).ok();
                client = make_client(auth.api_id).await?;
                client
                    .request_login_code(&auth.phone, &auth.api_hash)
                    .await?
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

    // Build file index
    let names: Vec<&str> = config.channels.iter().map(|c| c.name.as_str()).collect();
    let mut channel_peers = HashMap::new();
    let mut dialogs = client.iter_dialogs();
    while let Some(dialog) = dialogs.next().await? {
        if let Peer::Channel(ch) = dialog.peer() {
            if names.contains(&ch.title()) {
                if let Some(r) = ch.to_ref().await {
                    channel_peers.insert(ch.title().to_string(), r);
                }
            }
        }
    }

    let mut index: HashMap<String, Vec<FileEntry>> = HashMap::new();
    for entry in &config.channels {
        let name = &entry.name;
        let peer_ref = match channel_peers.get(name) {
            Some(r) => r.clone(),
            None => {
                eprintln!("Channel '{name}' not found in dialogs, skipping.");
                continue;
            }
        };

        print!("Indexing {name}...");
        io::stdout().flush().ok();

        let mut files = Vec::new();
        let mut messages = client.iter_messages(peer_ref);
        while let Some(msg) = messages.next().await? {
            if let Some(Media::Document(doc)) = msg.media() {
                let file_name = resolve_filename(&msg, &doc);
                let size = doc.size();
                let mime_type = doc.mime_type().unwrap_or("application/octet-stream").to_string();
                files.push(FileEntry { name: file_name, doc, size, mime_type });
            }
        }
        println!(" {} files", files.len());
        index.insert(name.clone(), files);
    }

    let state = Arc::new(AppState { client, index });

    let app = Router::new()
        .route("/", get(handle_root))
        .route("/:channel", get(handle_channel))
        .route("/:channel/", get(handle_channel))
        .route("/:channel/:filename", get(handle_download))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    println!("Serving on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
