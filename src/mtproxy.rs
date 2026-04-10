/// Local SOCKS5→MTProxy bridge.
///
/// grammers hardcodes `transport::Full` and only supports SOCKS5 proxies.  MTProxy requires
/// the obfuscated transport protocol and Telegram's "Intermediate" framing.  This module bridges
/// the two by:
///
///   1. Listening on a random local TCP port as a minimal no-auth SOCKS5 server.
///   2. For each incoming connection (one per Telegram DC grammers needs), connecting to the
///      configured MTProxy server and performing the obfuscated MTProxy handshake using the
///      provided secret.
///   3. Bidirectionally relaying data with Full↔Intermediate transport conversion:
///      - grammers → proxy: strip Full framing (seq + CRC32), wrap in Intermediate (length prefix),
///        encrypt with the TX cipher.
///      - proxy → grammers: decrypt with the RX cipher, unwrap Intermediate, wrap in Full
///        (adding a maintained seq counter and CRC32).
///
/// Key derivation (standard MTProxy, non-FakeTLS):
///   TX key = SHA-256(init[8..40] ‖ secret),  TX IV = init[40..56]
///   RX key = SHA-256(rev(init)[8..40] ‖ secret),  RX IV = rev(init)[40..56]
/// where `init` is the 64-byte random header the client generates.

#[allow(deprecated)] // GenericArray / KeyIvInit – see RustCrypto/block-ciphers#509
use aes::cipher::{KeyIvInit, StreamCipher, generic_array::GenericArray};
use crc32fast::Hasher as CrcHasher;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use log::{debug, warn};

type AesCtr = ctr::Ctr128BE<aes::Aes256>;

fn make_cipher(key: &[u8; 32], iv: &[u8; 16]) -> AesCtr {
    #[allow(deprecated)]
    AesCtr::new(GenericArray::from_slice(key), GenericArray::from_slice(iv))
}

/// Parse the hex-encoded MTProxy secret.  A leading `dd` byte (FakeTLS marker) is stripped;
/// the remaining 16 bytes are used as the standard obfuscated-transport secret.
fn parse_secret(hex_str: &str) -> anyhow::Result<[u8; 16]> {
    let s = hex_str.trim();
    let effective = if s.len() == 34 && s.starts_with("dd") { &s[2..] } else { s };
    anyhow::ensure!(
        effective.len() == 32,
        "MTProxy secret must be 32 hex chars (16 bytes); got {} chars",
        effective.len()
    );
    let bytes = hex_to_bytes(effective)?;
    Ok(bytes.try_into().unwrap())
}

fn hex_to_bytes(s: &str) -> anyhow::Result<Vec<u8>> {
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| anyhow::anyhow!("invalid hex char at position {i}"))
        })
        .collect()
}

/// Spawn the bridge listener.  Returns the local port that should be used as the SOCKS5
/// proxy URL passed to grammers (`socks5://127.0.0.1:<port>`).
pub async fn start_bridge(proxy_host: &str, proxy_port: u16, secret_hex: &str) -> anyhow::Result<u16> {
    let secret = parse_secret(secret_hex)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_port = listener.local_addr()?.port();
    tokio::spawn(run_bridge(listener, proxy_host.to_string(), proxy_port, secret));
    Ok(local_port)
}

async fn run_bridge(listener: TcpListener, proxy_host: String, proxy_port: u16, secret: [u8; 16]) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_conn(stream, proxy_host.clone(), proxy_port, secret));
            }
            Err(e) => {
                warn!("MTProxy bridge: accept error: {e}");
                break;
            }
        }
    }
}

async fn handle_conn(mut client: TcpStream, proxy_host: String, proxy_port: u16, secret: [u8; 16]) {
    if let Err(e) = do_handle_conn(&mut client, &proxy_host, proxy_port, &secret).await {
        debug!("MTProxy bridge: connection closed: {e}");
    }
}

async fn do_handle_conn(
    client: &mut TcpStream,
    proxy_host: &str,
    proxy_port: u16,
    secret: &[u8; 16],
) -> anyhow::Result<()> {
    socks5_accept(client).await?;
    let mut proxy = TcpStream::connect((proxy_host, proxy_port)).await?;
    let (tx, rx) = mtproxy_init(&mut proxy, secret).await?;
    bridge(client, proxy, tx, rx).await
}

// ---------------------------------------------------------------------------
// SOCKS5 server (no-auth, CONNECT only)
// ---------------------------------------------------------------------------

async fn socks5_accept(stream: &mut TcpStream) -> anyhow::Result<()> {
    // Greeting: [05, nmethods, method...]
    let mut hdr = [0u8; 2];
    stream.read_exact(&mut hdr).await?;
    anyhow::ensure!(hdr[0] == 5, "not SOCKS5 (got version {})", hdr[0]);
    let nmethods = hdr[1] as usize;
    let mut methods = vec![0u8; nmethods];
    stream.read_exact(&mut methods).await?;
    stream.write_all(&[5, 0]).await?; // accept, no auth

    // Request: [05, cmd=CONNECT, 00, atyp, ...]
    let mut req = [0u8; 4];
    stream.read_exact(&mut req).await?;
    anyhow::ensure!(req[1] == 1, "only SOCKS5 CONNECT is supported (cmd={})", req[1]);
    // Consume the target address (we always connect to the configured MTProxy server)
    match req[3] {
        1 => { let mut b = [0u8; 6]; stream.read_exact(&mut b).await?; }  // IPv4 (4) + port (2)
        3 => {
            let mut lb = [0u8; 1];
            stream.read_exact(&mut lb).await?;
            let mut b = vec![0u8; lb[0] as usize + 2]; // name + port
            stream.read_exact(&mut b).await?;
        }
        4 => { let mut b = [0u8; 18]; stream.read_exact(&mut b).await?; } // IPv6 (16) + port (2)
        t => anyhow::bail!("unknown SOCKS5 address type {t}"),
    }
    // Reply: success, bound to 0.0.0.0:0
    stream.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// MTProxy handshake
// ---------------------------------------------------------------------------

/// Forbidden patterns for the first 4 bytes of the obfuscated init (Telegram spec).
const FORBIDDEN_FIRST4: [[u8; 4]; 7] = [
    *b"HEAD",
    *b"POST",
    *b"GET ",
    *b"OPTI",
    [0x16, 0x03, 0x01, 0x02], // TLS ClientHello
    [0xdd, 0xdd, 0xdd, 0xdd], // Padded Intermediate tag
    [0xee, 0xee, 0xee, 0xee], // Intermediate tag
];

fn valid_init(init: &[u8; 64]) -> bool {
    if init[0] == 0xef { return false; }           // Abridged single-byte marker
    if init[4..8] == [0u8; 4] { return false; }   // looks like Full seq=0 prefix
    let first4: [u8; 4] = init[..4].try_into().unwrap();
    !FORBIDDEN_FIRST4.contains(&first4)
}

/// Perform the MTProxy client handshake: generate the obfuscated 64-byte init, derive
/// TX/RX ciphers using the secret, send the header, and return the ready ciphers.
async fn mtproxy_init(proxy: &mut TcpStream, secret: &[u8; 16]) -> anyhow::Result<(AesCtr, AesCtr)> {
    let mut init = [0u8; 64];
    loop {
        getrandom::getrandom(&mut init).map_err(|e| anyhow::anyhow!("getrandom: {e}"))?;
        // Transport tag: Intermediate (0xee 0xee 0xee 0xee)
        init[56..60].copy_from_slice(&[0xee, 0xee, 0xee, 0xee]);
        // Bytes 60..64: DC-id placeholder (0 = let proxy decide)
        init[60..64].fill(0);
        if valid_init(&init) { break; }
    }

    // TX key = SHA-256(init[8..40] ‖ secret)
    let tx_key: [u8; 32] = {
        let mut h = Sha256::new();
        h.update(&init[8..40]);
        h.update(secret);
        h.finalize().into()
    };
    let tx_iv: [u8; 16] = init[40..56].try_into().unwrap();

    // RX key = SHA-256(rev(init)[8..40] ‖ secret)
    let init_rev: [u8; 64] = { let mut r = init; r.reverse(); r };
    let rx_key: [u8; 32] = {
        let mut h = Sha256::new();
        h.update(&init_rev[8..40]);
        h.update(secret);
        h.finalize().into()
    };
    let rx_iv: [u8; 16] = init_rev[40..56].try_into().unwrap();

    let mut tx = make_cipher(&tx_key, &tx_iv);
    let rx = make_cipher(&rx_key, &rx_iv);

    // Encrypt the full 64-byte init to derive the last 8 bytes of the header.
    // After this, `tx` has consumed 64 keystream bytes and is ready for payload data.
    let mut enc = init;
    tx.apply_keystream(&mut enc);

    // Header: plaintext[0..56] + ciphertext[56..64]
    let mut header = init;
    header[56..64].copy_from_slice(&enc[56..64]);
    proxy.write_all(&header).await?;

    Ok((tx, rx))
}

// ---------------------------------------------------------------------------
// Bidirectional bridge with Full ↔ Intermediate conversion
// ---------------------------------------------------------------------------

async fn bridge(
    client: &mut TcpStream,
    proxy: TcpStream,
    mut tx: AesCtr,
    mut rx: AesCtr,
) -> anyhow::Result<()> {
    let (mut cr, mut cw) = client.split();
    let (mut pr, mut pw) = proxy.into_split();

    let c2p = async move { full_to_intermediate(&mut cr, &mut pw, &mut tx).await };
    let p2c = async move { intermediate_to_full(&mut pr, &mut cw, &mut rx).await };

    tokio::select! {
        r = c2p => r,
        r = p2c => r,
    }
}

/// Read Full-transport packets from `r`, convert to Intermediate, encrypt, write to `w`.
///
/// Full wire format:  [total_len: i32][seq: i32][payload…][crc32: u32]
/// where total_len = payload_len + 12.
///
/// Intermediate wire format: [payload_len: i32][payload…]
async fn full_to_intermediate(
    r: &mut (impl AsyncReadExt + Unpin),
    w: &mut (impl AsyncWriteExt + Unpin),
    cipher: &mut AesCtr,
) -> anyhow::Result<()> {
    let mut len_buf = [0u8; 4];
    loop {
        r.read_exact(&mut len_buf).await?;
        let total_len = i32::from_le_bytes(len_buf) as usize;
        anyhow::ensure!(total_len >= 12, "Full packet too short: {total_len}");

        // Read [seq(4)] [payload...] [crc(4)]
        let mut rest = vec![0u8; total_len - 4];
        r.read_exact(&mut rest).await?;

        // Extract payload, skipping seq(4) at the front and crc(4) at the back.
        let payload = &rest[4..rest.len() - 4];
        let payload_len = payload.len() as i32;

        // Build and encrypt an Intermediate packet.
        let mut pkt = Vec::with_capacity(4 + payload.len());
        pkt.extend_from_slice(&payload_len.to_le_bytes());
        pkt.extend_from_slice(payload);
        cipher.apply_keystream(&mut pkt);
        w.write_all(&pkt).await?;
    }
}

/// Read encrypted Intermediate packets from `r`, decrypt, wrap in Full, write to `w`.
async fn intermediate_to_full(
    r: &mut (impl AsyncReadExt + Unpin),
    w: &mut (impl AsyncWriteExt + Unpin),
    cipher: &mut AesCtr,
) -> anyhow::Result<()> {
    let mut recv_seq: i32 = 0;
    let mut len_buf = [0u8; 4];
    loop {
        r.read_exact(&mut len_buf).await?;
        cipher.apply_keystream(&mut len_buf);
        let payload_len = i32::from_le_bytes(len_buf) as usize;

        let mut payload = vec![0u8; payload_len];
        r.read_exact(&mut payload).await?;
        cipher.apply_keystream(&mut payload);

        // Build Full packet: [total_len][seq][payload][crc32]
        let total_len = (payload_len as i32) + 12;
        let seq = recv_seq;
        recv_seq += 1;

        let mut pkt = Vec::with_capacity(total_len as usize);
        pkt.extend_from_slice(&total_len.to_le_bytes());
        pkt.extend_from_slice(&seq.to_le_bytes());
        pkt.extend_from_slice(&payload);

        let crc = { let mut h = CrcHasher::new(); h.update(&pkt); h.finalize() };
        pkt.extend_from_slice(&crc.to_le_bytes());

        w.write_all(&pkt).await?;
    }
}
