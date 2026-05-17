use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use grammers_client::Client;
use grammers_client::client::UpdatesConfiguration;
use grammers_client::update::Update;
use grammers_session::types::{PeerId, PeerKind};
use grammers_session::updates::UpdatesLike;
use log::{debug, info, warn};
use tokio::sync::mpsc;

use crate::fuse::TgfsFS;
use crate::index::{AppState, ChannelSource};
use crate::indexer::{assemble_channel_files, message_to_raw_entry};
use crate::zip_cache::ZipCache;

/// Bind a Telegram update stream to the in-memory index. Mutates the channel's
/// `raw_entries` on `NewMessage`/`MessageEdited`/`MessageDeleted` and rebuilds
/// the FUSE subtree afterwards so kernel cache and inotify watchers see the
/// change.
pub struct Dispatcher {
    client: Client,
    state: Arc<AppState>,
    zip_cache: Arc<Mutex<ZipCache>>,
    fs: Option<TgfsFS>,
    /// Bare Telegram channel ID → indexing-key for `state.channels`. Built at
    /// startup; saved-messages and unknown channels are absent.
    channel_id_to_name: HashMap<i64, String>,
    /// Reverse of `state.dir_to_channel`. Same lifetime caveats apply.
    channel_to_dir: HashMap<String, String>,
}

impl Dispatcher {
    pub fn new(
        client: Client,
        state: Arc<AppState>,
        zip_cache: Arc<Mutex<ZipCache>>,
        fs: Option<TgfsFS>,
    ) -> Self {
        let mut channel_id_to_name = HashMap::new();
        for (name, lock) in &state.channels {
            let g = lock.read().unwrap();
            // Skip saved-messages: realtime tag/album resolution isn't implemented.
            if g.source != ChannelSource::RegularChannel { continue; }
            if let Some(peer) = g.peer {
                if peer.id.kind() == PeerKind::Channel {
                    channel_id_to_name.insert(peer.id.bare_id(), name.clone());
                }
            }
        }
        let channel_to_dir: HashMap<String, String> = state
            .dir_to_channel
            .iter()
            .map(|(dir, name)| (name.clone(), dir.clone()))
            .collect();
        Self { client, state, zip_cache, fs, channel_id_to_name, channel_to_dir }
    }

    /// Subscribe to Telegram updates and process them until the stream errors.
    /// Intended to run as a long-lived `tokio::spawn` task.
    pub async fn run(self, updates: mpsc::UnboundedReceiver<UpdatesLike>) {
        let mut stream = self
            .client
            .stream_updates(updates, UpdatesConfiguration::default())
            .await;
        info!("realtime: dispatcher started");
        loop {
            match stream.next().await {
                Ok(update) => {
                    if let Err(e) = self.handle(update).await {
                        warn!("realtime: update handling failed: {:?}", e);
                    }
                }
                Err(e) => {
                    warn!("realtime: update stream ended: {:?}", e);
                    return;
                }
            }
        }
    }

    async fn handle(&self, update: Update) -> anyhow::Result<()> {
        match update {
            Update::NewMessage(msg) | Update::MessageEdited(msg) => {
                let peer_id = msg.peer_id();
                let channel_name = match self.channel_for_peer(peer_id) {
                    Some(n) => n,
                    None => return Ok(()),
                };
                self.upsert_message(&channel_name, &msg).await?;
            }
            Update::MessageDeleted(del) => {
                let channel_id = match del.channel_id() {
                    Some(id) => id,
                    // DeleteMessages (no channel context) cannot be routed to a
                    // specific channel from the update payload alone — skip.
                    None => return Ok(()),
                };
                let channel_name = match self.channel_id_to_name.get(&channel_id) {
                    Some(n) => n.clone(),
                    None => return Ok(()),
                };
                let msg_ids: Vec<i32> = del.messages().to_vec();
                self.delete_messages(&channel_name, &msg_ids).await?;
            }
            _ => {}
        }
        Ok(())
    }

    fn channel_for_peer(&self, peer_id: PeerId) -> Option<String> {
        if peer_id.kind() != PeerKind::Channel { return None; }
        self.channel_id_to_name.get(&peer_id.bare_id()).cloned()
    }

    async fn upsert_message(
        &self,
        channel_name: &str,
        msg: &grammers_client::message::Message,
    ) -> anyhow::Result<()> {
        let archive_view = {
            let lock = self.state.channels.get(channel_name)
                .ok_or_else(|| anyhow::anyhow!("unknown channel {}", channel_name))?;
            lock.read().unwrap().archive_view
        };

        let raw = message_to_raw_entry(
            &self.client,
            msg,
            archive_view,
            &self.state.mime_pool,
            &self.zip_cache,
        ).await;

        let (mut raw_entries, collapse) = {
            let lock = self.state.channels.get(channel_name).unwrap();
            let g = lock.read().unwrap();
            (g.raw_entries.clone(), g.collapse_by_prefix)
        };

        let msg_id = msg.id();
        let had_old = raw_entries.contains_key(&msg_id);
        match raw {
            Some(r) => {
                debug!("realtime: upsert msg_id={} in channel='{}'", msg_id, channel_name);
                raw_entries.insert(msg_id, r);
            }
            None if had_old => {
                debug!("realtime: edit removed media, dropping msg_id={} in channel='{}'", msg_id, channel_name);
                raw_entries.remove(&msg_id);
            }
            None => return Ok(()),
        }

        let new_files = assemble_channel_files(
            &self.client,
            &raw_entries,
            archive_view,
            collapse,
            &self.zip_cache,
        ).await;

        {
            let lock = self.state.channels.get(channel_name).unwrap();
            let mut g = lock.write().unwrap();
            g.raw_entries = raw_entries;
            g.files = Arc::new(new_files);
        }

        self.notify_fuse(channel_name);
        Ok(())
    }

    async fn delete_messages(&self, channel_name: &str, msg_ids: &[i32]) -> anyhow::Result<()> {
        let (mut raw_entries, archive_view, collapse) = {
            let lock = self.state.channels.get(channel_name)
                .ok_or_else(|| anyhow::anyhow!("unknown channel {}", channel_name))?;
            let g = lock.read().unwrap();
            (g.raw_entries.clone(), g.archive_view, g.collapse_by_prefix)
        };

        let mut changed = false;
        for id in msg_ids {
            if raw_entries.remove(id).is_some() {
                debug!("realtime: deleted msg_id={} in channel='{}'", id, channel_name);
                changed = true;
            }
        }
        if !changed { return Ok(()); }

        let new_files = assemble_channel_files(
            &self.client,
            &raw_entries,
            archive_view,
            collapse,
            &self.zip_cache,
        ).await;

        {
            let lock = self.state.channels.get(channel_name).unwrap();
            let mut g = lock.write().unwrap();
            g.raw_entries = raw_entries;
            g.files = Arc::new(new_files);
        }

        self.notify_fuse(channel_name);
        Ok(())
    }

    fn notify_fuse(&self, channel_name: &str) {
        let fs = match &self.fs { Some(f) => f, None => return };
        let dir = match self.channel_to_dir.get(channel_name) {
            Some(d) => d,
            None => return,
        };
        fs.rebuild_channel(dir);
    }
}
