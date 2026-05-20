//! Interactive destination picker, invoked when `tgup` is run without
//! `-c/--channel`. Lists every Telegram dialog the account can send to,
//! pinning channels declared in `config.channels` to the top. Renders a
//! fixed 8-row scrollable window on stderr.

use std::io;

use anyhow::{anyhow, bail};
use console::{Key, Term};
use grammers_client::peer::Peer;
use grammers_client::Client;
use grammers_session::types::{ChannelKind, PeerRef};

use tgfs::config::Config;

/// Height of the visible selection window. The list scrolls within this
/// height; the highlighted item is always kept on-screen.
const WINDOW: usize = 8;

#[derive(Clone)]
struct Item {
    label: String,
    name: String,
    peer_ref: PeerRef,
}

/// Connect-side entry point. Returns the chosen `(name, peer_ref)`.
pub async fn pick_destination(client: &Client, config: &Config) -> anyhow::Result<(String, PeerRef)> {
    let items = collect_dialogs(client, config).await?;
    if items.is_empty() {
        bail!("no dialogs found on this account to pick from");
    }
    let term = Term::stderr();
    if !term.features().is_attended() {
        bail!("--channel was omitted but stderr is not a TTY — pass -c <name> instead");
    }
    let idx = run_picker(&term, &items)?;
    let chosen = &items[idx];
    Ok((chosen.name.clone(), chosen.peer_ref))
}

async fn collect_dialogs(client: &Client, config: &Config) -> anyhow::Result<Vec<Item>> {
    let mut all: Vec<(String, &'static str, PeerRef)> = Vec::new();
    let mut dialogs = client.iter_dialogs();
    while let Some(d) = dialogs.next().await? {
        let peer = d.peer();
        if !can_post(peer) { continue; }
        let kind = match peer {
            Peer::User(_) => "chat",
            Peer::Group(_) => "group",
            Peer::Channel(_) => "channel",
        };
        let name = match peer.name() {
            Some(n) if !n.trim().is_empty() => n.to_string(),
            _ => continue,
        };
        all.push((name, kind, d.peer_ref()));
    }

    // Pin channels declared in the config to the top, in the order they
    // appear there. Everything else keeps its dialog order (Telegram returns
    // dialogs by recent activity).
    let config_names: Vec<&str> = config.channels.iter().map(|c| c.name.as_str()).collect();
    let mut pinned: Vec<Item> = Vec::new();
    for cname in &config_names {
        if let Some(pos) = all.iter().position(|(n, _, _)| n == *cname) {
            let (n, k, r) = all.remove(pos);
            pinned.push(Item {
                label: format!("[{}] ★ {}", k, n),
                name: n,
                peer_ref: r,
            });
        }
    }
    let rest: Vec<Item> = all.into_iter()
        .map(|(n, k, r)| Item {
            label: format!("[{}]   {}", k, n),
            name: n,
            peer_ref: r,
        })
        .collect();

    let mut out = pinned;
    out.extend(rest);
    Ok(out)
}

fn run_picker(term: &Term, items: &[Item]) -> anyhow::Result<usize> {
    let mut selected: usize = 0;
    let mut offset: usize = 0;
    let win = WINDOW.min(items.len()).max(1);

    let header_count = print_header(term, items)?;
    print_window(term, items, selected, offset, win)?;

    loop {
        let key = term.read_key().map_err(io_to_anyhow)?;
        match key {
            Key::ArrowUp | Key::Char('k') => {
                if selected > 0 {
                    selected -= 1;
                    if selected < offset { offset = selected; }
                }
            }
            Key::ArrowDown | Key::Char('j') => {
                if selected + 1 < items.len() {
                    selected += 1;
                    if selected >= offset + win {
                        offset = selected + 1 - win;
                    }
                }
            }
            Key::Home => { selected = 0; offset = 0; }
            Key::End => {
                selected = items.len() - 1;
                offset = items.len().saturating_sub(win);
            }
            Key::PageUp => {
                selected = selected.saturating_sub(win);
                if selected < offset { offset = selected; }
            }
            Key::PageDown => {
                selected = std::cmp::min(items.len() - 1, selected + win);
                if selected >= offset + win {
                    offset = selected + 1 - win;
                }
            }
            Key::Enter => break,
            Key::Escape | Key::Char('q') => bail!("destination picker cancelled"),
            _ => continue,
        }
        // Redraw only the window; leave the header in place above it.
        term.clear_last_lines(win).map_err(io_to_anyhow)?;
        print_window(term, items, selected, offset, win)?;
        let _ = header_count;
    }
    // Clear the picker (header + window) so subsequent stderr output starts
    // on a clean line.
    term.clear_last_lines(win + header_count).map_err(io_to_anyhow)?;
    Ok(selected)
}

fn print_header(term: &Term, items: &[Item]) -> anyhow::Result<usize> {
    let lines = [
        format!(
            "Pick a destination ({} dialog{}). ↑↓ to navigate · Enter to pick · Esc to cancel",
            items.len(), if items.len() == 1 { "" } else { "s" },
        ),
        "★ = listed in tgfs.yml".to_string(),
    ];
    for l in &lines {
        term.write_line(l).map_err(io_to_anyhow)?;
    }
    Ok(lines.len())
}

fn print_window(
    term: &Term,
    items: &[Item],
    selected: usize,
    offset: usize,
    win: usize,
) -> anyhow::Result<()> {
    let end = std::cmp::min(offset + win, items.len());
    for i in offset..end {
        let marker = if i == selected { ">" } else { " " };
        // Style the selected line so it stands out even without color (bold ▸).
        let line = format!("{} {}", marker, items[i].label);
        let styled = if i == selected {
            console::style(line).reverse().to_string()
        } else {
            line
        };
        term.write_line(&styled).map_err(io_to_anyhow)?;
    }
    // Pad the window so clear_last_lines(win) on the next iteration always
    // erases the exact same number of rows.
    for _ in end..(offset + win) {
        term.write_line("").map_err(io_to_anyhow)?;
    }
    Ok(())
}

fn io_to_anyhow(e: io::Error) -> anyhow::Error { anyhow!("terminal I/O: {}", e) }

/// Skip dialogs where this account can't actually send messages — read-only
/// broadcast channels (anyone can subscribe; only admins with the
/// `post_messages` right can post) are the common case worth filtering. For
/// User/Group/Megagroup/Gigagroup dialogs we assume the account can post;
/// banned-rights/slow-mode edge cases aren't worth a round-trip per dialog
/// just to hide a single picker entry.
fn can_post(peer: &Peer) -> bool {
    match peer {
        Peer::User(_) | Peer::Group(_) => true,
        Peer::Channel(ch) => match ch.kind() {
            Some(ChannelKind::Broadcast) => {
                ch.admin_rights().map(|r| r.post_messages).unwrap_or(false)
            }
            // Megagroup / gigagroup behave like groups for posting.
            Some(_) => true,
            None => true,
        },
    }
}
