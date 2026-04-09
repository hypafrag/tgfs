use std::collections::HashMap;
use std::path::PathBuf;
use std::fs;
use std::io::{Read, Write};
use serde::{Serialize, Deserialize};
use crate::index::ArchiveFileEntry;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone)]
pub struct ZipCacheKey {
    pub name: String,
    pub size: usize,
}

#[derive(Serialize, Deserialize, Default)]
struct PersistedMap(Vec<(ZipCacheKey, Vec<ArchiveFileEntry>)>);

pub struct ZipCache {
    map: HashMap<ZipCacheKey, Vec<ArchiveFileEntry>>,
    path: PathBuf,
    dirty: bool,
}

impl ZipCache {
    pub fn load(path: &str) -> Self {
        let p = PathBuf::from(path);
        if p.exists() {
            // support gzipped JSON or plain JSON depending on file extension
            let mut data = String::new();
            if p.extension().map(|s| s == "gz").unwrap_or(false) {
                if let Ok(f) = fs::File::open(&p) {
                    let mut d = GzDecoder::new(f);
                    if d.read_to_string(&mut data).is_ok() {
                        if let Ok(pm) = serde_json::from_str::<PersistedMap>(&data) {
                            let mut map = HashMap::new();
                            for (k, v) in pm.0.into_iter() {
                                map.insert(k, v);
                            }
                            return ZipCache { map, path: p, dirty: false };
                        }
                    }
                }
            } else if let Ok(s) = fs::read_to_string(&p) {
                data = s;
                if let Ok(pm) = serde_json::from_str::<PersistedMap>(&data) {
                    let mut map = HashMap::new();
                    for (k, v) in pm.0.into_iter() {
                        map.insert(k, v);
                    }
                    return ZipCache { map, path: p, dirty: false };
                }
            }
        }
        ZipCache { map: HashMap::new(), path: p, dirty: false }
    }

    pub fn get(&self, key: &ZipCacheKey) -> Option<Vec<ArchiveFileEntry>> {
        self.map.get(key).cloned()
    }

    pub fn insert(&mut self, key: ZipCacheKey, entries: Vec<ArchiveFileEntry>) {
        self.map.insert(key, entries);
        self.dirty = true;
    }

    pub fn save(&mut self) -> anyhow::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let mut vec = Vec::with_capacity(self.map.len());
        for (k, v) in self.map.iter() {
            vec.push((k.clone(), v.clone()));
        }
        let pm = PersistedMap(vec);
        let s = serde_json::to_string_pretty(&pm)?;
        // write gzipped output if path ends with .gz, otherwise plain
        if self.path.extension().map(|s| s == "gz").unwrap_or(false) {
            let f = fs::File::create(&self.path)?;
            let mut enc = GzEncoder::new(f, Compression::default());
            enc.write_all(s.as_bytes())?;
            enc.finish()?;
        } else {
            fs::write(&self.path, s)?;
        }
        self.dirty = false;
        Ok(())
    }
}
