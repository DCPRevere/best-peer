//! TLS on the door, and the client side of `best-peer approve`. Rustls on ring: no system library, so the binary
//! stays one file on every target. The certificate is the operator's (`BEST_PEER_TLS_CERT` and `BEST_PEER_TLS_KEY`,
//! PEM files) or one the peer makes for itself at first start and keeps in its data directory, named in its log so
//! that every client can pin it. The peer checks nothing about a certificate beyond that it loads: whose name it
//! carries is the operator's business. ACME is not here: a private cloud has its own CA or pins.
//!
//! What the peer serves and where is written to `<data>/door` at start, so `best-peer approve`, which runs on the
//! same machine with the same data directory, knows where to go and which certificate to trust: that one, by its
//! bytes, and no other.

use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, ServerConfig, SignatureScheme};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

/// Whether the door serves TLS, and with whose certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tls {
    /// Plain HTTP: the endpoint is `http://` and no certificate was given.
    Off,
    /// The operator's certificate chain and private key, PEM files.
    Given { cert: PathBuf, key: PathBuf },
    /// The peer's own, made at first start in its data directory.
    Own,
}

impl Tls {
    /// The decision from the endpoint's scheme and the two variables: a certificate comes with its key or not at
    /// all, and serves an `https://` endpoint only.
    pub fn decide(endpoint: &str, cert: Option<&str>, key: Option<&str>) -> Result<Self, String> {
        let https = endpoint.starts_with("https://");
        match (cert, key) {
            (Some(cert), Some(key)) if https => Ok(Tls::Given {
                cert: PathBuf::from(cert),
                key: PathBuf::from(key),
            }),
            (Some(_), Some(_)) => Err(format!(
                "BEST_PEER_ENDPOINT is {endpoint}, but a certificate is set: the endpoint a certificate serves starts with https://"
            )),
            (Some(_), None) => Err("BEST_PEER_TLS_CERT is set without BEST_PEER_TLS_KEY: name the key's PEM file too, or unset both and the peer makes its own certificate".into()),
            (None, Some(_)) => Err("BEST_PEER_TLS_KEY is set without BEST_PEER_TLS_CERT: name the certificate's PEM file too, or unset both and the peer makes its own certificate".into()),
            (None, None) if https => Ok(Tls::Own),
            (None, None) => Ok(Tls::Off),
        }
    }
}

/// What the peer serves and where, for `best-peer approve`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Record {
    pub listen: SocketAddr,
    pub endpoint: String,
    /// The PEM file of the certificate the door serves, when it serves TLS.
    pub certificate: Option<PathBuf>,
}

impl Record {
    const FILE: &'static str = "door";

    pub fn write(&self, dir: &Path) -> std::io::Result<()> {
        std::fs::write(dir.join(Self::FILE), serde_json::to_vec_pretty(self)?)
    }

    pub fn read(dir: &Path) -> std::io::Result<Self> {
        let text = std::fs::read_to_string(dir.join(Self::FILE))?;
        serde_json::from_str(&text).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Where `approve` connects: the listen address, or this machine's loopback when the peer listens on every
    /// interface, which is no address to dial.
    pub fn dial(&self) -> SocketAddr {
        let ip = match self.listen.ip() {
            ip if !ip.is_unspecified() => ip,
            std::net::IpAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
            std::net::IpAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
        };
        SocketAddr::new(ip, self.listen.port())
    }
}

/// The names the peer's own certificate carries: this machine's loopback by its three names, the host the endpoint
/// names, and the address the door listens on when it is one.
pub fn names_for(endpoint: &str, listen: &SocketAddr) -> Vec<String> {
    let mut names = vec!["localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string()];
    let host = endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or_default();
    let host = match host.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or_default(),
        None => host.split(':').next().unwrap_or_default(),
    };
    let mut wanted = vec![host.to_string()];
    if !listen.ip().is_unspecified() {
        wanted.push(listen.ip().to_string());
    }
    for name in wanted {
        if !name.is_empty() && !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// The server's TLS configuration and the PEM file of the certificate it serves, or nothing when the door is plain
/// HTTP. The operator's files are loaded as given; the peer's own are made under `<dir>/tls` the first time and read
/// back after.
pub fn serving(tls: &Tls, dir: &Path, names: &[String]) -> Result<Option<(Arc<ServerConfig>, PathBuf)>, String> {
    let (cert, key) = match tls {
        Tls::Off => return Ok(None),
        Tls::Given { cert, key } => (cert.clone(), key.clone()),
        Tls::Own => own(&dir.join("tls"), names)?,
    };
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert)
        .and_then(|certs| certs.collect())
        .map_err(|e| format!("cannot read the certificate in {}: {e}", cert.display()))?;
    if chain.is_empty() {
        return Err(format!(
            "{} holds no certificate: a PEM file with at least one CERTIFICATE block",
            cert.display()
        ));
    }
    let key_der = PrivateKeyDer::from_pem_file(&key)
        .map_err(|e| format!("cannot read the private key in {}: {e}", key.display()))?;
    let mut config = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS is not available: {e}"))?
        .with_no_client_auth()
        .with_single_cert(chain, key_der)
        .map_err(|e| {
            format!(
                "the certificate in {} and the key in {} do not make a TLS identity: {e}",
                cert.display(),
                key.display()
            )
        })?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Some((Arc::new(config), cert)))
}

/// The peer's own certificate and key under `dir`: made once, self-signed on an ECDSA P-256 key, valid ten years,
/// the key file readable by its owner alone.
fn own(dir: &Path, names: &[String]) -> Result<(PathBuf, PathBuf), String> {
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    if cert.is_file() && key.is_file() {
        return Ok((cert, key));
    }
    std::fs::create_dir_all(dir).map_err(|e| format!("cannot make {}: {e}", dir.display()))?;
    let mut params = rcgen::CertificateParams::new(names.to_vec())
        .map_err(|e| format!("cannot put {names:?} on a certificate: {e}"))?;
    params.distinguished_name.push(rcgen::DnType::CommonName, "best-peer");
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::days(1);
    params.not_after = now + time::Duration::days(10 * 365);
    let pair = rcgen::KeyPair::generate().map_err(|e| format!("cannot make a key: {e}"))?;
    let made = params
        .self_signed(&pair)
        .map_err(|e| format!("cannot make a certificate: {e}"))?;
    write_private(&key, pair.serialize_pem().as_bytes()).map_err(|e| format!("cannot write {}: {e}", key.display()))?;
    std::fs::write(&cert, made.pem()).map_err(|e| format!("cannot write {}: {e}", cert.display()))?;
    Ok((cert, key))
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)?.write_all(bytes)
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A request to the peer at `addr`: over TLS when `pin` names the PEM file of the certificate it serves, and then
/// that certificate is the only one accepted, by its bytes, whatever name it carries. The answer's status and its
/// body as JSON, or nothing when the body is not JSON.
pub async fn call(
    addr: SocketAddr,
    pin: Option<&Path>,
    req: Request<Body>,
) -> Result<(StatusCode, serde_json::Value), String> {
    let tcp = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("cannot reach the peer at {addr}: {e}"))?;
    let Some(pin) = pin else {
        return exchange(tcp, req).await;
    };
    let pinned = CertificateDer::from_pem_file(pin)
        .map_err(|e| format!("cannot read the certificate the peer serves, {}: {e}", pin.display()))?;
    let config = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS is not available: {e}"))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(Pinned {
            cert: pinned,
            provider: provider(),
        }))
        .with_no_client_auth();
    let name = ServerName::IpAddress(addr.ip().into());
    let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(name, tcp)
        .await
        .map_err(|e| {
            format!(
                "the peer at {addr} does not serve the certificate in {}: {e}",
                pin.display()
            )
        })?;
    exchange(stream, req).await
}

async fn exchange<S>(stream: S, req: Request<Body>) -> Result<(StatusCode, serde_json::Value), String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|e| format!("the peer did not speak HTTP: {e}"))?;
    tokio::spawn(connection);
    let res = sender
        .send_request(req)
        .await
        .map_err(|e| format!("the peer did not answer: {e}"))?;
    let status = res.status();
    let bytes = axum::body::to_bytes(Body::new(res.into_body()), 1 << 20)
        .await
        .map_err(|e| format!("the peer's answer could not be read: {e}"))?;
    Ok((status, serde_json::from_slice(&bytes).unwrap_or_default()))
}

/// Accepts one certificate, by its bytes, and no other: the one the peer on this machine serves.
#[derive(Debug)]
struct Pinned {
    cert: CertificateDer<'static>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.cert.as_ref() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_certificate_comes_with_its_key_and_serves_https_only() {
        assert_eq!(Tls::decide("http://127.0.0.1:7777", None, None).unwrap(), Tls::Off);
        assert_eq!(Tls::decide("https://127.0.0.1:7777", None, None).unwrap(), Tls::Own);
        assert_eq!(
            Tls::decide("https://peer.example.com", Some("c.pem"), Some("k.pem")).unwrap(),
            Tls::Given {
                cert: "c.pem".into(),
                key: "k.pem".into()
            }
        );
        let why = Tls::decide("http://peer.example.com", Some("c.pem"), Some("k.pem")).unwrap_err();
        assert!(why.contains("https://"), "{why}");
        let why = Tls::decide("https://peer.example.com", Some("c.pem"), None).unwrap_err();
        assert!(why.contains("BEST_PEER_TLS_KEY"), "{why}");
        let why = Tls::decide("https://peer.example.com", None, Some("k.pem")).unwrap_err();
        assert!(why.contains("BEST_PEER_TLS_CERT"), "{why}");
    }

    #[test]
    fn the_own_certificate_names_loopback_the_endpoint_host_and_the_listen_address() {
        let names = names_for("https://peer.example.com:7777/", &"0.0.0.0:7777".parse().unwrap());
        assert_eq!(names, ["localhost", "127.0.0.1", "::1", "peer.example.com"]);
        let names = names_for("https://10.0.0.5:7777", &"10.0.0.5:7777".parse().unwrap());
        assert_eq!(names, ["localhost", "127.0.0.1", "::1", "10.0.0.5"]);
        let names = names_for("https://[::1]:7777", &"[::]:7777".parse().unwrap());
        assert_eq!(names, ["localhost", "127.0.0.1", "::1"]);
        let names = names_for("https://box.lan", &"192.168.1.9:443".parse().unwrap());
        assert_eq!(names, ["localhost", "127.0.0.1", "::1", "box.lan", "192.168.1.9"]);
    }

    #[test]
    fn the_own_certificate_is_made_once_and_the_operators_files_are_refused_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let names = names_for("https://127.0.0.1:7777", &"127.0.0.1:7777".parse().unwrap());
        let (_, cert) = serving(&Tls::Own, dir.path(), &names).unwrap().unwrap();
        assert_eq!(cert, dir.path().join("tls").join("cert.pem"));
        let first = std::fs::read(&cert).unwrap();
        assert!(String::from_utf8_lossy(&first).starts_with("-----BEGIN CERTIFICATE-----"));
        let (_, again) = serving(&Tls::Own, dir.path(), &names).unwrap().unwrap();
        assert_eq!(
            std::fs::read(&again).unwrap(),
            first,
            "a second start made a new certificate"
        );

        // The operator's files: the ones just made load; a missing one is refused by its path.
        let given = Tls::Given {
            cert: cert.clone(),
            key: dir.path().join("tls").join("key.pem"),
        };
        assert!(serving(&given, dir.path(), &[]).unwrap().is_some());
        let missing = Tls::Given {
            cert: dir.path().join("nowhere.pem"),
            key: dir.path().join("tls").join("key.pem"),
        };
        let why = serving(&missing, dir.path(), &[]).unwrap_err();
        assert!(why.contains("nowhere.pem"), "{why}");
        assert!(serving(&Tls::Off, dir.path(), &[]).unwrap().is_none());
    }

    #[test]
    fn the_record_says_where_approve_dials() {
        let dir = tempfile::tempdir().unwrap();
        let record = Record {
            listen: "0.0.0.0:7777".parse().unwrap(),
            endpoint: "https://peer.example.com:7777".into(),
            certificate: Some(dir.path().join("cert.pem")),
        };
        record.write(dir.path()).unwrap();
        let read = Record::read(dir.path()).unwrap();
        assert_eq!(read, record);
        assert_eq!(read.dial().to_string(), "127.0.0.1:7777");
        let v6 = Record {
            listen: "[::]:7777".parse().unwrap(),
            ..read
        };
        assert_eq!(v6.dial().to_string(), "[::1]:7777");
        assert!(Record::read(&dir.path().join("none")).is_err());
    }
}
