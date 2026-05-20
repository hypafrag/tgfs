//! Thin TVmaze REST client used by `--tvshow` to fill in episode titles
//! when hunch can't recover them (e.g. `s01e06_TS-19.avi` — hunch reads
//! "TS-19" as a release-tag-style code and returns no title, but TVmaze
//! correctly reports the canonical episode name "TS-19").
//!
//! Endpoints used (both unauthenticated, public, rate-limited politely):
//!   * `GET /search/shows?q=<title>` — returns `[{score, show: {id, name, …}}, …]`
//!   * `GET /shows/<id>/episodes` — returns `[{season, number, name, …}, …]`
//!
//! Lookups are cached per-show within a process, so a season folder with
//! N episodes triggers at most two HTTP calls (one search + one episodes
//! list), not 2*N.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context as _;
use log::{debug, warn};
use serde::Deserialize;

const BASE: &str = "https://api.tvmaze.com";

#[derive(Deserialize)]
struct SearchHit {
    show: SearchHitShow,
}

#[derive(Deserialize)]
struct SearchHitShow {
    id: u64,
}

#[derive(Deserialize)]
struct EpisodeResp {
    season: i32,
    number: Option<i32>,
    name: Option<String>,
}

#[derive(Debug)]
struct ShowEpisodes {
    /// `(season, episode_number) → episode_title`
    titles: HashMap<(i32, i32), String>,
}

#[derive(Debug)]
pub struct Tvmaze {
    client: reqwest::Client,
    /// Cache keyed by case-insensitive show title. `None` means "looked up
    /// and got nothing" — don't retry within the same process.
    cache: HashMap<String, Option<ShowEpisodes>>,
}

impl Tvmaze {
    pub fn new() -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("tgup/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(15))
            .build()
            .context("building reqwest client for TVmaze")?;
        Ok(Self { client, cache: HashMap::new() })
    }

    /// Look up `(show_title, season, episode_number)` → episode title.
    /// Returns `None` when the show isn't found, the episode is missing,
    /// the API call fails, or the title is empty. Errors are logged at
    /// warn level so the upload plan still proceeds with the bare form.
    pub async fn episode_title(
        &mut self,
        show_title: &str,
        season: i32,
        episode: i32,
    ) -> Option<String> {
        let key = show_title.to_lowercase();
        if !self.cache.contains_key(&key) {
            let v = match self.fetch_show(show_title).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("TVmaze lookup for {show_title:?} failed: {e:#}");
                    None
                }
            };
            self.cache.insert(key.clone(), v);
        }
        self.cache.get(&key)?.as_ref()?
            .titles.get(&(season, episode))
            .cloned()
    }

    async fn fetch_show(&self, title: &str) -> anyhow::Result<Option<ShowEpisodes>> {
        let url = format!(
            "{BASE}/search/shows?q={}",
            urlencoding::encode(title),
        );
        debug!("TVmaze GET {url}");
        let hits: Vec<SearchHit> = self.client.get(&url)
            .send().await.context("TVmaze /search/shows: request failed")?
            .error_for_status().context("TVmaze /search/shows: bad status")?
            .json().await.context("TVmaze /search/shows: invalid JSON")?;
        let Some(top) = hits.into_iter().next() else { return Ok(None); };
        let id = top.show.id;

        let eps_url = format!("{BASE}/shows/{id}/episodes");
        debug!("TVmaze GET {eps_url}");
        let eps: Vec<EpisodeResp> = self.client.get(&eps_url)
            .send().await.context("TVmaze /shows/{id}/episodes: request failed")?
            .error_for_status().context("TVmaze /shows/{id}/episodes: bad status")?
            .json().await.context("TVmaze /shows/{id}/episodes: invalid JSON")?;
        let mut titles = HashMap::new();
        for e in eps {
            let (Some(n), Some(name)) = (e.number, e.name) else { continue };
            if !name.is_empty() {
                titles.insert((e.season, n), name);
            }
        }
        Ok(Some(ShowEpisodes { titles }))
    }
}
