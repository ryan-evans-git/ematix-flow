//! AWS Glue Schema Registry wire format constants and codec helpers.
//!
//! Distinct from the Confluent wire format already handled in
//! [`crate::kafka_backend`]. Glue uses:
//!
//! ```text
//! ┌────────┬────────────────────┬────────────┬─────────────────┐
//! │  0x03  │  16-byte schema    │ 1-byte     │  payload (Avro/ │
//! │ header │  UUID (big-endian) │ codec id   │  Protobuf bytes)│
//! └────────┴────────────────────┴────────────┴─────────────────┘
//!   off 0     off 1..=16            off 17       off 18..
//! ```
//!
//! Confluent's wire format by contrast is:
//!
//! ```text
//! ┌────────┬──────────────────────┬──────────────────────────┐
//! │  0x00  │  4-byte BE schema id │  payload                 │
//! └────────┴──────────────────────┴──────────────────────────┘
//! ```
//!
//! Picking the wrong framing for your registry will corrupt every
//! message on the first call — the schema-id lookup will resolve to
//! garbage, decode will surface as "incompatible schema" errors. The
//! framing is dispatched on the connection ``kind`` (``schema_registry``
//! vs ``glue_schema_registry``) so users opt in explicitly via their
//! typed connection class.
//!
//! This module only carries the framing primitives (header constant,
//! payload extract, frame build). The actual schema fetch and IAM auth
//! live in the Python side via boto3 / aws-glue-schema-registry today;
//! a future revision can move that to ``aws-sdk-rust`` in the Rust
//! runner if the Python round-trip becomes a bottleneck.

use uuid::Uuid;

/// Magic byte that prefixes every Glue-framed Kafka message.
///
/// The Confluent wire format uses ``0x00`` for the same purpose;
/// distinguishing the two framings on read is as simple as switching
/// on this first byte (if the registry kind is known at decode time
/// — which it is, since it's bound to the connection).
pub const GLUE_HEADER_BYTE: u8 = 0x03;

/// Total framing overhead in bytes: 1 (header) + 16 (UUID) + 1 (codec).
pub const GLUE_HEADER_LEN: usize = 1 + 16 + 1;

/// AWS Glue compression codec identifiers.
///
/// Glue defines two compression schemes for the payload bytes after
/// the header:
///
/// - ``0x00`` — uncompressed (used by every codec ematix-flow ships
///   today; the codec field is purely informational on read).
/// - ``0x05`` — zlib-compressed. ematix-flow doesn't currently encode
///   with this, but it must accept it on decode for cross-vendor
///   compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GlueCodec {
    None = 0x00,
    Zlib = 0x05,
}

impl GlueCodec {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x00 => Some(GlueCodec::None),
            0x05 => Some(GlueCodec::Zlib),
            _ => None,
        }
    }
}

/// A parsed Glue-framed message: the schema UUID and the payload
/// slice (the bytes after the header).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlueFrame<'a> {
    pub schema_uuid: Uuid,
    pub codec: GlueCodec,
    pub payload: &'a [u8],
}

/// Errors that can come out of [`parse_glue_frame`].
#[derive(Debug, thiserror::Error)]
pub enum GlueFrameError {
    #[error("Glue-framed message must be at least {GLUE_HEADER_LEN} bytes, got {0}")]
    TooShort(usize),
    #[error("Glue header byte must be 0x{GLUE_HEADER_BYTE:02x}, got 0x{0:02x}")]
    BadHeaderByte(u8),
    #[error("Glue codec byte 0x{0:02x} is not recognised")]
    UnknownCodec(u8),
}

/// Parse a Glue-framed Kafka message. Returns the schema UUID and a
/// slice of the payload bytes. Does not decompress the payload; that's
/// the caller's responsibility based on [`GlueFrame::codec`].
pub fn parse_glue_frame(bytes: &[u8]) -> Result<GlueFrame<'_>, GlueFrameError> {
    if bytes.len() < GLUE_HEADER_LEN {
        return Err(GlueFrameError::TooShort(bytes.len()));
    }
    if bytes[0] != GLUE_HEADER_BYTE {
        return Err(GlueFrameError::BadHeaderByte(bytes[0]));
    }
    // UUID is big-endian 16 bytes per the Glue spec; Uuid::from_bytes
    // expects big-endian-major form, which matches.
    let uuid_bytes: [u8; 16] = bytes[1..17].try_into().unwrap();
    let schema_uuid = Uuid::from_bytes(uuid_bytes);
    let codec = GlueCodec::from_byte(bytes[17]).ok_or(GlueFrameError::UnknownCodec(bytes[17]))?;
    let payload = &bytes[GLUE_HEADER_LEN..];
    Ok(GlueFrame {
        schema_uuid,
        codec,
        payload,
    })
}

/// Build a Glue-framed Kafka message: ``0x03`` + UUID + codec + payload.
///
/// Allocates a new ``Vec<u8>`` sized exactly to ``GLUE_HEADER_LEN +
/// payload.len()``. For hot-path encoding the caller should prefer
/// [`write_glue_frame_into`] which writes into a pre-allocated buffer.
pub fn build_glue_frame(schema_uuid: Uuid, codec: GlueCodec, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(GLUE_HEADER_LEN + payload.len());
    write_glue_frame_into(&mut out, schema_uuid, codec, payload);
    out
}

/// Write a Glue frame into the caller's buffer. Appends, does not
/// truncate.
pub fn write_glue_frame_into(
    out: &mut Vec<u8>,
    schema_uuid: Uuid,
    codec: GlueCodec,
    payload: &[u8],
) {
    out.push(GLUE_HEADER_BYTE);
    out.extend_from_slice(schema_uuid.as_bytes());
    out.push(codec as u8);
    out.extend_from_slice(payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_uuid() -> Uuid {
        // Deterministic UUID for round-trip tests.
        Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap()
    }

    #[test]
    fn round_trip_minimal_payload() {
        let uuid = sample_uuid();
        let payload = b"hello, world";
        let framed = build_glue_frame(uuid, GlueCodec::None, payload);
        assert_eq!(framed.len(), GLUE_HEADER_LEN + payload.len());

        let parsed = parse_glue_frame(&framed).unwrap();
        assert_eq!(parsed.schema_uuid, uuid);
        assert_eq!(parsed.codec, GlueCodec::None);
        assert_eq!(parsed.payload, payload);
    }

    #[test]
    fn empty_payload_round_trip() {
        let uuid = sample_uuid();
        let framed = build_glue_frame(uuid, GlueCodec::None, &[]);
        assert_eq!(framed.len(), GLUE_HEADER_LEN);

        let parsed = parse_glue_frame(&framed).unwrap();
        assert_eq!(parsed.schema_uuid, uuid);
        assert_eq!(parsed.payload, &[] as &[u8]);
    }

    #[test]
    fn zlib_codec_round_trip() {
        let uuid = sample_uuid();
        // Caller is responsible for actually compressing; this just
        // round-trips the codec byte.
        let payload = vec![0x78, 0x9c, 0xff, 0xff]; // arbitrary "zlib-shaped" bytes
        let framed = build_glue_frame(uuid, GlueCodec::Zlib, &payload);

        let parsed = parse_glue_frame(&framed).unwrap();
        assert_eq!(parsed.codec, GlueCodec::Zlib);
        assert_eq!(parsed.payload, &payload[..]);
    }

    #[test]
    fn rejects_too_short() {
        assert!(matches!(
            parse_glue_frame(&[]),
            Err(GlueFrameError::TooShort(0))
        ));
        // Header byte alone, no UUID.
        assert!(matches!(
            parse_glue_frame(&[GLUE_HEADER_BYTE; 1]),
            Err(GlueFrameError::TooShort(_))
        ));
        // Header + 15 UUID bytes — one short.
        let mut buf = vec![GLUE_HEADER_BYTE];
        buf.extend_from_slice(&[0xab; 16]);
        // No codec byte at position 17.
        assert!(matches!(
            parse_glue_frame(&buf),
            Err(GlueFrameError::TooShort(17))
        ));
    }

    #[test]
    fn rejects_bad_header_byte() {
        // 0x00 is Confluent's header — must be rejected if we're
        // expecting Glue framing.
        let mut buf = vec![0x00];
        buf.extend_from_slice(&[0; 17]);
        assert!(matches!(
            parse_glue_frame(&buf),
            Err(GlueFrameError::BadHeaderByte(0x00))
        ));
    }

    #[test]
    fn rejects_unknown_codec() {
        let mut buf = vec![GLUE_HEADER_BYTE];
        buf.extend_from_slice(sample_uuid().as_bytes());
        buf.push(0xff); // not a known codec
        buf.extend_from_slice(b"payload");
        assert!(matches!(
            parse_glue_frame(&buf),
            Err(GlueFrameError::UnknownCodec(0xff))
        ));
    }

    #[test]
    fn glue_header_byte_distinct_from_confluent() {
        // Sanity check: the whole point of this module is that we
        // distinguish from Confluent at byte 0.
        const CONFLUENT_HEADER_BYTE: u8 = 0x00;
        assert_ne!(GLUE_HEADER_BYTE, CONFLUENT_HEADER_BYTE);
    }
}
