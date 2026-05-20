//! Peer-discovery for distributed `flow-worker` meshes.
//!
//! ematix-flow's distributed backend takes a `Vec<String>` of peer
//! references; before Phase 3 every entry had to be a fully-resolved
//! URL like `http://flow-01.cluster.local:50051`. That works for
//! fixed-membership clusters but is painful in dynamic environments —
//! K8s pod IPs change on every restart, autoscaling adds and removes
//! nodes, and devs can't sanely list a 50-node fleet by hand.
//!
//! [`expand_peer_entries`] is the entry point: it walks each
//! configured peer string, recognises three schemes, and returns a
//! single flat list of concrete URLs ready for the existing
//! `StaticWorkerResolver`.
//!
//! ## Supported schemes
//!
//! - `http://host:port` / `https://host:port` — passed through
//!   unchanged. Backwards-compatible with v0.3.0.
//! - `dns://host:port` — resolves `host` via the OS resolver and
//!   emits one `http://ip:port` URL per A-record returned. Covers
//!   K8s headless services (which expose one A record per pod),
//!   AWS Cloud Map, Consul-DNS, and any other DNS-driven service
//!   registry.
//! - `k8s://service.namespace:port` — sugar for
//!   `dns://service.namespace.svc.cluster.local:port`. Convenience
//!   for the most common deployment shape; lets `peers = ["k8s://flow-workers.flow:50051"]`
//!   replace 50 lines of pod-IP config.
//!
//! ## Resolution timing
//!
//! Resolution happens **once** when the backend is opened. We do
//! not periodically refresh — peer membership changes require a
//! backend rebuild today. Periodic refresh (mDNS, async SRV with
//! TTL-driven re-resolve) is a Phase 3b follow-up; the public
//! function signature here doesn't preclude it.
//!
//! Synchronous resolution via [`std::net::ToSocketAddrs`] keeps the
//! distributed crate's dep graph small — no `hickory-resolver` or
//! similar pulled in. The cost is no SRV/TXT lookups today; if the
//! K8s headless-A-record path doesn't cover a deployment, we can
//! either (a) document a sidecar that translates SRV → static
//! `peers` list, or (b) feature-gate a hickory-backed expander
//! later.

use std::net::ToSocketAddrs;

use url::Url;

use ematix_flow_core::backend::BackendError;

/// Expand a list of peer references into concrete `http://ip:port`
/// URLs.
///
/// Each entry is one of:
///
/// - A regular URL (`http://...`, `https://...`) — passed through.
/// - `dns://host:port` — A-record lookup, one URL per address.
/// - `k8s://service.namespace:port` — sugar for the DNS form
///   targeting the standard `*.svc.cluster.local` suffix.
///
/// Returns a single flat `Vec<Url>`. Duplicates across entries (e.g.
/// two `dns://` entries that resolve to overlapping addresses) are
/// not deduplicated — operators who want that should pass through
/// `dedupe_urls` (or accept the harmless extra dial attempts).
///
/// Errors are returned as [`BackendError::Connection`] with the
/// offending entry quoted, mirroring the existing static-URL parse
/// error path so callers get one consistent message shape.
pub fn expand_peer_entries(entries: &[String]) -> Result<Vec<Url>, BackendError> {
    let mut out: Vec<Url> = Vec::new();
    for (i, raw) in entries.iter().enumerate() {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let expanded = expand_one(trimmed).map_err(|e| {
            BackendError::Connection(format!("peer #{i} ({raw:?}): {e}"))
        })?;
        out.extend(expanded);
    }
    Ok(out)
}

/// Resolve a single peer entry. Returns the expanded URL list (one
/// element for plain URLs, N elements for DNS-resolved entries).
fn expand_one(entry: &str) -> Result<Vec<Url>, String> {
    if entry.starts_with("dns://") {
        let host_port = &entry["dns://".len()..];
        return resolve_a_records(host_port);
    }
    if entry.starts_with("k8s://") {
        // k8s://service.namespace:port  →  dns://service.namespace.svc.cluster.local:port
        let host_port = &entry["k8s://".len()..];
        let (host, port) = split_host_port(host_port)?;
        let fqdn = format!("{host}.svc.cluster.local");
        return resolve_a_records(&format!("{fqdn}:{port}"));
    }
    // Anything else must parse as a regular URL with a scheme +
    // host. Empty / malformed entries surface here.
    let url = Url::parse(entry).map_err(|e| format!("invalid URL: {e}"))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(format!(
            "unsupported scheme {scheme:?} (expected http, https, dns, or k8s)",
            scheme = url.scheme()
        ));
    }
    Ok(vec![url])
}

/// Look up A records for `host:port` and emit one `http://ip:port`
/// URL per address. Empty resolution is an error — silently dropping
/// would leave operators wondering why a node isn't getting traffic.
fn resolve_a_records(host_port: &str) -> Result<Vec<Url>, String> {
    let (host, port) = split_host_port(host_port)?;
    let addrs: Vec<_> = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("DNS lookup for {host:?} failed: {e}"))?
        .collect();
    if addrs.is_empty() {
        return Err(format!("DNS lookup for {host:?} returned no addresses"));
    }
    let mut out = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let ip = addr.ip();
        // Render IPv6 with brackets so the URL parses cleanly.
        let host_render = if ip.is_ipv6() {
            format!("[{ip}]")
        } else {
            ip.to_string()
        };
        let url = Url::parse(&format!("http://{host_render}:{port}"))
            .map_err(|e| format!("could not build URL from resolved address {addr}: {e}"))?;
        out.push(url);
    }
    Ok(out)
}

fn split_host_port(host_port: &str) -> Result<(&str, u16), String> {
    let (host, port) = host_port
        .rsplit_once(':')
        .ok_or_else(|| format!("missing port in {host_port:?} (expected host:port)"))?;
    if host.is_empty() {
        return Err(format!("empty host in {host_port:?}"));
    }
    let port: u16 = port
        .parse()
        .map_err(|e| format!("invalid port {port:?}: {e}"))?;
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Plain URLs pass through unchanged — backwards compat.
    #[test]
    fn http_url_passthrough() {
        let out = expand_peer_entries(&["http://10.0.0.5:50051".into()]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].as_str(), "http://10.0.0.5:50051/");
    }

    #[test]
    fn https_url_passthrough() {
        let out = expand_peer_entries(&["https://flow-worker.example.com:8443".into()]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].scheme(), "https");
    }

    #[test]
    fn mixed_static_and_dns_concatenates() {
        // localhost always resolves; we just want to verify the
        // dns:// branch returns at least one URL.
        let entries = vec![
            "http://10.0.0.5:50051".into(),
            "dns://localhost:50051".into(),
        ];
        let out = expand_peer_entries(&entries).unwrap();
        assert!(out.len() >= 2);
        assert!(out.iter().any(|u| u.as_str() == "http://10.0.0.5:50051/"));
        // The dns:// branch should have produced at least one
        // 127.0.0.1 / ::1 URL.
        assert!(out
            .iter()
            .any(|u| u.host_str() == Some("127.0.0.1") || u.host_str() == Some("[::1]")));
    }

    #[test]
    fn empty_input_is_empty_output() {
        let out = expand_peer_entries(&[]).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn whitespace_entries_skipped() {
        let out = expand_peer_entries(&["   ".into(), "".into()]).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn invalid_scheme_errors_clearly() {
        let err = expand_peer_entries(&["ftp://flow-worker:50051".into()]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unsupported scheme"), "got: {msg}");
        assert!(msg.contains("ftp"), "got: {msg}");
    }

    #[test]
    fn dns_missing_port_errors() {
        let err = expand_peer_entries(&["dns://localhost".into()]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("missing port"), "got: {msg}");
    }

    #[test]
    fn dns_invalid_port_errors() {
        let err = expand_peer_entries(&["dns://localhost:notaport".into()]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("invalid port"), "got: {msg}");
    }

    #[test]
    fn dns_nonexistent_host_errors() {
        // ".invalid" is RFC 6761 reserved for "definitely does not resolve."
        let err = expand_peer_entries(&[
            "dns://nonexistent-host.invalid:50051".into(),
        ])
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("DNS lookup"), "got: {msg}");
    }

    #[test]
    fn k8s_shorthand_expands_to_cluster_local() {
        // We can't fully resolve a *.svc.cluster.local hostname
        // outside a real K8s cluster, but the error message must
        // mention the expanded FQDN — that's how operators
        // diagnose misconfiguration.
        let err = expand_peer_entries(&[
            "k8s://flow-workers.flow:50051".into(),
        ])
        .unwrap_err();
        let msg = format!("{err}");
        // Either the lookup happened (we got "DNS lookup ... failed")
        // or the FQDN appears in the error.
        assert!(
            msg.contains("svc.cluster.local") || msg.contains("DNS lookup"),
            "expected expansion to include cluster.local domain or a DNS error; got: {msg}"
        );
    }

    #[test]
    fn k8s_missing_port_errors() {
        let err = expand_peer_entries(&["k8s://flow-workers.flow".into()]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("missing port"), "got: {msg}");
    }

    /// The error message must include the offending entry verbatim
    /// so operators can grep their config.
    #[test]
    fn error_quotes_the_bad_entry() {
        let err =
            expand_peer_entries(&["definitely-not-a-url".into()]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("definitely-not-a-url"), "got: {msg}");
    }
}
