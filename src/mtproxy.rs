/// Local SOCKS5 → MTProxy bridge, supporting both standard obfuscated and FakeTLS modes.
///
/// # Standard obfuscated (`secret` = 32 hex chars, no prefix)
/// Client sends a 64-byte random "init" header.  Cipher keys are derived as
/// `SHA-256(init[8..40] ‖ secret)` / `SHA-256(rev(init)[8..40] ‖ secret)`.
/// The TX cipher advances 64 positions for the header; post-handshake data flows
/// as raw AES-CTR–encrypted Intermediate MTProto packets.
///
/// # FakeTLS (`secret` = "dd" + 32 hex chars)
/// Client sends a fake TLS 1.3 ClientHello whose `random[28..32]` carries
/// `HMAC-SHA256(secret, hello_with_mac_zeroed)[0..4]` and whose `session_id`
/// (32 random bytes) carries the client's key material.  The server responds with
/// a fake ServerHello (containing `server_random`) and a ChangeCipherSpec.
/// Cipher keys are derived from the 64-byte concatenation `session_id ‖ server_random`
/// using the same SHA-256 formula.  Post-handshake data is wrapped in TLS
/// ApplicationData records (`17 03 03 [len]`) on both sides.

#[allow(deprecated)]
use aes::cipher::{generic_array::GenericArray, KeyIvInit, StreamCipher};
use crc32fast::Hasher as CrcHasher;
use log::{debug, warn};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

type AesCtr = ctr::Ctr128BE<aes::Aes256>;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Spawn the bridge.  Returns the local port (use as `socks5://127.0.0.1:<port>`).
pub async fn start_bridge(proxy_host: &str, proxy_port: u16, secret_hex: &str) -> anyhow::Result<u16> {
    let (secret, faketls) = parse_secret(secret_hex)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_port = listener.local_addr()?.port();
    tokio::spawn(run_bridge(listener, proxy_host.to_string(), proxy_port, secret, faketls));
    Ok(local_port)
}

// ---------------------------------------------------------------------------
// Secret parsing
// ---------------------------------------------------------------------------

/// Returns `(16-byte secret, is_faketls)`.
/// A leading `dd` byte (34-char hex) signals FakeTLS; anything else is standard.
fn parse_secret(hex_str: &str) -> anyhow::Result<([u8; 16], bool)> {
    let s = hex_str.trim();
    let (effective, faketls) = if s.len() == 34 && s.starts_with("dd") {
        (&s[2..], true)
    } else {
        (s, false)
    };
    anyhow::ensure!(
        effective.len() == 32,
        "MTProxy secret must be 32 hex chars (16 bytes); got {} chars",
        effective.len()
    );
    let bytes = hex_decode(effective)?;
    Ok((bytes.try_into().unwrap(), faketls))
}

fn hex_decode(s: &str) -> anyhow::Result<Vec<u8>> {
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| anyhow::anyhow!("invalid hex at position {i}"))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Bridge runner
// ---------------------------------------------------------------------------

async fn run_bridge(
    listener: TcpListener,
    proxy_host: String,
    proxy_port: u16,
    secret: [u8; 16],
    faketls: bool,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_conn(stream, proxy_host.clone(), proxy_port, secret, faketls));
            }
            Err(e) => {
                warn!("MTProxy bridge: accept error: {e}");
                break;
            }
        }
    }
}

async fn handle_conn(
    mut client: TcpStream,
    proxy_host: String,
    proxy_port: u16,
    secret: [u8; 16],
    faketls: bool,
) {
    if let Err(e) =
        do_handle_conn(&mut client, &proxy_host, proxy_port, &secret, faketls).await
    {
        debug!("MTProxy bridge: connection closed: {e}");
    }
}

async fn do_handle_conn(
    client: &mut TcpStream,
    proxy_host: &str,
    proxy_port: u16,
    secret: &[u8; 16],
    faketls: bool,
) -> anyhow::Result<()> {
    socks5_accept(client).await?;
    let mut proxy = TcpStream::connect((proxy_host, proxy_port)).await?;
    let (tx, rx) = if faketls {
        faketls_handshake(&mut proxy, proxy_host, secret).await?
    } else {
        obfuscated_init(&mut proxy, secret).await?
    };
    relay(client, proxy, tx, rx, faketls).await
}

// ---------------------------------------------------------------------------
// SOCKS5 server (no-auth, CONNECT only)
// ---------------------------------------------------------------------------

async fn socks5_accept(s: &mut TcpStream) -> anyhow::Result<()> {
    let mut hdr = [0u8; 2];
    s.read_exact(&mut hdr).await?;
    anyhow::ensure!(hdr[0] == 5, "not SOCKS5");
    let mut methods = vec![0u8; hdr[1] as usize];
    s.read_exact(&mut methods).await?;
    s.write_all(&[5, 0]).await?;

    let mut req = [0u8; 4];
    s.read_exact(&mut req).await?;
    anyhow::ensure!(req[1] == 1, "only CONNECT supported");
    match req[3] {
        1 => { let mut b = [0u8; 6];  s.read_exact(&mut b).await?; }
        3 => {
            let mut lb = [0u8; 1]; s.read_exact(&mut lb).await?;
            let mut b = vec![0u8; lb[0] as usize + 2]; s.read_exact(&mut b).await?;
        }
        4 => { let mut b = [0u8; 18]; s.read_exact(&mut b).await?; }
        t => anyhow::bail!("unknown SOCKS5 atyp {t}"),
    }
    s.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Cipher helpers
// ---------------------------------------------------------------------------

fn make_cipher(key: &[u8; 32], iv: &[u8; 16]) -> AesCtr {
    #[allow(deprecated)]
    AesCtr::new(GenericArray::from_slice(key), GenericArray::from_slice(iv))
}

/// Derive the TX and RX AES-CTR ciphers from a 64-byte init and the 16-byte secret.
/// Both ciphers start at keystream position 0.
fn derive_ciphers(init: &[u8; 64], secret: &[u8; 16]) -> (AesCtr, AesCtr) {
    let mut rev = *init;
    rev.reverse();

    let tx_key: [u8; 32] = sha256_two(&init[8..40], secret);
    let tx_iv:  [u8; 16] = init[40..56].try_into().unwrap();
    let rx_key: [u8; 32] = sha256_two(&rev[8..40], secret);
    let rx_iv:  [u8; 16] = rev[40..56].try_into().unwrap();

    (make_cipher(&tx_key, &tx_iv), make_cipher(&rx_key, &rx_iv))
}

fn sha256_two(a: &[u8], b: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(a);
    h.update(b);
    h.finalize().into()
}

/// HMAC-SHA256 with a key ≤ 64 bytes (key is zero-padded to the block size).
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() <= 64 { k[..key.len()].copy_from_slice(key); }
    else { k[..32].copy_from_slice(&sha256_two(key, &[])); }

    let mut ipad = k;
    let mut opad = k;
    for i in 0..64 { ipad[i] ^= 0x36; opad[i] ^= 0x5c; }

    let inner: [u8; 32] = {
        let mut h = Sha256::new();
        h.update(ipad);
        h.update(data);
        h.finalize().into()
    };
    let mut h = Sha256::new();
    h.update(opad);
    h.update(inner);
    h.finalize().into()
}

fn getrandom_bytes(buf: &mut [u8]) -> anyhow::Result<()> {
    getrandom::getrandom(buf).map_err(|e| anyhow::anyhow!("getrandom: {e}"))
}

// ---------------------------------------------------------------------------
// Standard obfuscated handshake
// ---------------------------------------------------------------------------

const FORBIDDEN_FIRST4: [[u8; 4]; 7] = [
    *b"HEAD", *b"POST", *b"GET ", *b"OPTI",
    [0x16, 0x03, 0x01, 0x02],
    [0xdd, 0xdd, 0xdd, 0xdd],
    [0xee, 0xee, 0xee, 0xee],
];

async fn obfuscated_init(proxy: &mut TcpStream, secret: &[u8; 16]) -> anyhow::Result<(AesCtr, AesCtr)> {
    let mut init = [0u8; 64];
    loop {
        getrandom_bytes(&mut init)?;
        init[56..60].copy_from_slice(&[0xee, 0xee, 0xee, 0xee]); // Intermediate tag
        init[60..64].fill(0);
        let first4: [u8; 4] = init[..4].try_into().unwrap();
        if init[0] != 0xef && init[4..8] != [0u8; 4] && !FORBIDDEN_FIRST4.contains(&first4) {
            break;
        }
    }

    let (mut tx, rx) = derive_ciphers(&init, secret);

    // Encrypt the full init; last 8 bytes become the authenticator in the header.
    let mut enc = init;
    tx.apply_keystream(&mut enc); // tx now at position 64

    let mut header = init;
    header[56..64].copy_from_slice(&enc[56..64]);
    proxy.write_all(&header).await?;

    Ok((tx, rx))
}

// ---------------------------------------------------------------------------
// FakeTLS handshake
// ---------------------------------------------------------------------------

async fn faketls_handshake(
    proxy: &mut TcpStream,
    proxy_host: &str,
    secret: &[u8; 16],
) -> anyhow::Result<(AesCtr, AesCtr)> {
    let mut session_id = [0u8; 32];
    getrandom_bytes(&mut session_id)?;

    let hello = build_client_hello(proxy_host, &session_id, secret)?;
    proxy.write_all(&hello).await?;

    let server_random = read_server_handshake(proxy).await?;

    // 64-byte init = session_id ‖ server_random (same key-derivation formula as standard).
    let mut init = [0u8; 64];
    init[..32].copy_from_slice(&session_id);
    init[32..].copy_from_slice(&server_random);

    // Ciphers start at position 0 (no header advancement needed).
    Ok(derive_ciphers(&init, secret))
}

/// Build a TLS 1.3 ClientHello record with an HMAC authenticator in random[28..32].
fn build_client_hello(host: &str, session_id: &[u8; 32], secret: &[u8; 16]) -> anyhow::Result<Vec<u8>> {
    // --- random field (timestamp ‖ rand ‖ mac_placeholder) ---
    let mut random = [0u8; 32];
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32;
    random[..4].copy_from_slice(&ts.to_be_bytes());
    getrandom_bytes(&mut random[4..28])?;
    // random[28..32] stays zero — filled with HMAC after the record is built.

    // --- extensions ---
    let mut exts: Vec<u8> = Vec::new();

    // server_name (SNI) — omit for bare IP addresses.
    if host.parse::<std::net::IpAddr>().is_err() && !host.is_empty() {
        let name = host.as_bytes();
        let nl = name.len() as u16;
        exts.extend_from_slice(&[0x00, 0x00]);                       // type: server_name
        exts.extend_from_slice(&(nl + 5).to_be_bytes());             // ext data len
        exts.extend_from_slice(&(nl + 3).to_be_bytes());             // list len
        exts.push(0x00);                                              // name type: host_name
        exts.extend_from_slice(&nl.to_be_bytes());
        exts.extend_from_slice(name);
    }

    // supported_groups: x25519, secp256r1, secp384r1
    exts.extend_from_slice(&[
        0x00, 0x0a, 0x00, 0x08, 0x00, 0x06,
        0x00, 0x1d, 0x00, 0x17, 0x00, 0x18,
    ]);

    // ec_point_formats: uncompressed
    exts.extend_from_slice(&[0x00, 0x0b, 0x00, 0x02, 0x01, 0x00]);

    // session_ticket: empty
    exts.extend_from_slice(&[0x00, 0x23, 0x00, 0x00]);

    // supported_versions: TLS 1.3, TLS 1.2
    exts.extend_from_slice(&[0x00, 0x2b, 0x00, 0x05, 0x04, 0x03, 0x04, 0x03, 0x03]);

    // signature_algorithms (9 pairs)
    exts.extend_from_slice(&[
        0x00, 0x0d, 0x00, 0x14, 0x00, 0x12,
        0x04, 0x03, 0x08, 0x04, 0x04, 0x01,
        0x05, 0x03, 0x08, 0x05, 0x05, 0x01,
        0x08, 0x06, 0x06, 0x01, 0x02, 0x01,
    ]);

    // key_share: x25519 with a random 32-byte public key
    let mut ks = [0u8; 32];
    getrandom_bytes(&mut ks)?;
    exts.extend_from_slice(&[0x00, 0x33, 0x00, 0x26, 0x00, 0x24, 0x00, 0x1d, 0x00, 0x20]);
    exts.extend_from_slice(&ks);

    // psk_key_exchange_modes: psk_dhe_ke
    exts.extend_from_slice(&[0x00, 0x2d, 0x00, 0x02, 0x01, 0x01]);

    // --- cipher suites ---
    let suites: &[u8] = &[
        0x13, 0x01, 0x13, 0x02, 0x13, 0x03,  // TLS 1.3 suites
        0xC0, 0x2B, 0xC0, 0x2F, 0xC0, 0x2C, 0xC0, 0x30,
        0xC0, 0x13, 0xC0, 0x14, 0x00, 0x9C, 0x00, 0x9D,
        0x00, 0x2F, 0x00, 0x35,
    ];

    // --- ClientHello body ---
    let mut ch: Vec<u8> = Vec::new();
    ch.extend_from_slice(&[0x03, 0x03]);          // legacy version TLS 1.2
    ch.extend_from_slice(&random);
    ch.push(32);                                   // session_id length
    ch.extend_from_slice(session_id);
    ch.extend_from_slice(&(suites.len() as u16).to_be_bytes());
    ch.extend_from_slice(suites);
    ch.extend_from_slice(&[0x01, 0x00]);           // compression: null
    ch.extend_from_slice(&(exts.len() as u16).to_be_bytes());
    ch.extend_from_slice(&exts);

    // --- Handshake header ---
    let ch_len = ch.len() as u32;
    let mut hs: Vec<u8> = vec![
        0x01,                                      // HandshakeType: ClientHello
        ((ch_len >> 16) & 0xff) as u8,
        ((ch_len >>  8) & 0xff) as u8,
        ( ch_len        & 0xff) as u8,
    ];
    hs.extend_from_slice(&ch);

    // --- TLS record ---
    let hs_len = hs.len() as u16;
    let mut record: Vec<u8> = vec![0x16, 0x03, 0x01]; // Handshake, TLS 1.0
    record.extend_from_slice(&hs_len.to_be_bytes());
    record.extend_from_slice(&hs);

    // --- HMAC authenticator at random[28..32] within the record ---
    // random sits at: record_hdr(5) + hs_hdr(4) + legacy_ver(2) = offset 11
    let mac_offset = 5 + 4 + 2 + 28; // = 39
    record[mac_offset..mac_offset + 4].fill(0);    // ensure zeroed before computing
    let mac = hmac_sha256(secret, &record);
    record[mac_offset..mac_offset + 4].copy_from_slice(&mac[..4]);

    Ok(record)
}

/// Read TLS records from the server until the fake handshake is complete.
/// Returns the 32-byte `server_random` from the ServerHello.
/// Stops after seeing a ChangeCipherSpec (0x14) or the first ApplicationData (0x17).
async fn read_server_handshake(proxy: &mut TcpStream) -> anyhow::Result<[u8; 32]> {
    let mut server_random: Option<[u8; 32]> = None;

    loop {
        let mut hdr = [0u8; 5];
        proxy.read_exact(&mut hdr).await?;
        let ct  = hdr[0];
        let len = u16::from_be_bytes([hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len];
        proxy.read_exact(&mut body).await?;

        match ct {
            0x16 => { // Handshake
                // HandshakeType 0x02 = ServerHello; random is at body[6..38]
                if body.len() >= 38 && body[0] == 0x02 {
                    server_random = Some(body[6..38].try_into()?);
                }
                // Other handshake messages (EncryptedExtensions etc.) are skipped.
            }
            0x14 => { // ChangeCipherSpec — handshake done
                return server_random
                    .ok_or_else(|| anyhow::anyhow!("FakeTLS: ChangeCipherSpec before ServerHello"));
            }
            0x17 => { // ApplicationData — server skipped ChangeCipherSpec, data starts now
                return server_random
                    .ok_or_else(|| anyhow::anyhow!("FakeTLS: ApplicationData before ServerHello"));
            }
            0x15 => anyhow::bail!("FakeTLS: TLS Alert from server: {:02x?}", &body[..2.min(len)]),
            ct   => anyhow::bail!("FakeTLS: unexpected record type 0x{ct:02x}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Bidirectional relay (Full ↔ Intermediate, with optional TLS AppData wrapping)
// ---------------------------------------------------------------------------

async fn relay(
    client: &mut TcpStream,
    proxy: TcpStream,
    mut tx: AesCtr,
    mut rx: AesCtr,
    faketls: bool,
) -> anyhow::Result<()> {
    let (mut cr, mut cw) = client.split();
    let (mut pr, mut pw) = proxy.into_split();

    if faketls {
        let c2p = async move { c2p_faketls(&mut cr, &mut pw, &mut tx).await };
        let p2c = async move { p2c_faketls(&mut pr, &mut cw, &mut rx).await };
        tokio::select! { r = c2p => r, r = p2c => r }
    } else {
        let c2p = async move { c2p_plain(&mut cr, &mut pw, &mut tx).await };
        let p2c = async move { p2c_plain(&mut pr, &mut cw, &mut rx).await };
        tokio::select! { r = c2p => r, r = p2c => r }
    }
}

// --- standard (plain) mode ---

/// Full → Intermediate → cipher → proxy
async fn c2p_plain(
    r: &mut (impl AsyncReadExt + Unpin),
    w: &mut (impl AsyncWriteExt + Unpin),
    cipher: &mut AesCtr,
) -> anyhow::Result<()> {
    let mut len_buf = [0u8; 4];
    loop {
        r.read_exact(&mut len_buf).await?;
        let total = i32::from_le_bytes(len_buf) as usize;
        anyhow::ensure!(total >= 12, "Full packet too short: {total}");
        let mut rest = vec![0u8; total - 4];
        r.read_exact(&mut rest).await?;
        let payload = &rest[4..rest.len() - 4]; // strip seq + crc

        let mut pkt = Vec::with_capacity(4 + payload.len());
        pkt.extend_from_slice(&(payload.len() as i32).to_le_bytes());
        pkt.extend_from_slice(payload);
        cipher.apply_keystream(&mut pkt);
        w.write_all(&pkt).await?;
    }
}

/// proxy → cipher → Intermediate → Full
async fn p2c_plain(
    r: &mut (impl AsyncReadExt + Unpin),
    w: &mut (impl AsyncWriteExt + Unpin),
    cipher: &mut AesCtr,
) -> anyhow::Result<()> {
    let mut recv_seq: i32 = 0;
    let mut len_buf = [0u8; 4];
    loop {
        r.read_exact(&mut len_buf).await?;
        cipher.apply_keystream(&mut len_buf);
        let plen = i32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; plen];
        r.read_exact(&mut payload).await?;
        cipher.apply_keystream(&mut payload);
        w.write_all(&wrap_full(&payload, recv_seq)).await?;
        recv_seq += 1;
    }
}

// --- FakeTLS mode (same Full↔Intermediate, but wrapped in TLS AppData records) ---

/// Full → Intermediate → cipher → TLS AppData → proxy
async fn c2p_faketls(
    r: &mut (impl AsyncReadExt + Unpin),
    w: &mut (impl AsyncWriteExt + Unpin),
    cipher: &mut AesCtr,
) -> anyhow::Result<()> {
    let mut len_buf = [0u8; 4];
    loop {
        r.read_exact(&mut len_buf).await?;
        let total = i32::from_le_bytes(len_buf) as usize;
        anyhow::ensure!(total >= 12, "Full packet too short: {total}");
        let mut rest = vec![0u8; total - 4];
        r.read_exact(&mut rest).await?;
        let payload = &rest[4..rest.len() - 4];

        let mut pkt = Vec::with_capacity(4 + payload.len());
        pkt.extend_from_slice(&(payload.len() as i32).to_le_bytes());
        pkt.extend_from_slice(payload);
        cipher.apply_keystream(&mut pkt);

        // Wrap in TLS ApplicationData record
        let rec_len = pkt.len() as u16;
        let mut rec = vec![0x17, 0x03, 0x03];
        rec.extend_from_slice(&rec_len.to_be_bytes());
        rec.extend_from_slice(&pkt);
        w.write_all(&rec).await?;
    }
}

/// proxy → TLS AppData → cipher → Intermediate → Full
async fn p2c_faketls(
    r: &mut (impl AsyncReadExt + Unpin),
    w: &mut (impl AsyncWriteExt + Unpin),
    cipher: &mut AesCtr,
) -> anyhow::Result<()> {
    let mut recv_seq: i32 = 0;
    loop {
        let mut hdr = [0u8; 5];
        r.read_exact(&mut hdr).await?;
        anyhow::ensure!(
            hdr[0] == 0x17,
            "FakeTLS: expected ApplicationData (0x17), got 0x{:02x}",
            hdr[0]
        );
        let rec_len = u16::from_be_bytes([hdr[3], hdr[4]]) as usize;
        let mut enc = vec![0u8; rec_len];
        r.read_exact(&mut enc).await?;
        cipher.apply_keystream(&mut enc);

        // Parse as Intermediate: [payload_len: i32][payload...]
        anyhow::ensure!(enc.len() >= 4, "Intermediate record too short");
        let plen = i32::from_le_bytes(enc[..4].try_into()?) as usize;
        anyhow::ensure!(enc.len() >= 4 + plen, "truncated Intermediate payload");
        w.write_all(&wrap_full(&enc[4..4 + plen], recv_seq)).await?;
        recv_seq += 1;
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a Full-transport packet from a raw MTProto payload and a seq counter.
fn wrap_full(payload: &[u8], seq: i32) -> Vec<u8> {
    let total = (payload.len() as i32) + 12;
    let mut pkt = Vec::with_capacity(total as usize);
    pkt.extend_from_slice(&total.to_le_bytes());
    pkt.extend_from_slice(&seq.to_le_bytes());
    pkt.extend_from_slice(payload);
    let crc = { let mut h = CrcHasher::new(); h.update(&pkt); h.finalize() };
    pkt.extend_from_slice(&crc.to_le_bytes());
    pkt
}
