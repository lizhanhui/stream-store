//! S3 extent file codec: encode sealed extents for S3 upload and decode headers
//! for S3 range-read.
//!
//! ## File Layout
//!
//! ```text
//! ┌─ Header (fixed 64 bytes) ──────────────────────────┐
//! │  magic, version, flags, stream_id, start_offset,    │
//! │  end_offset, record_count, index_interval,          │
//! │  index_entry_count, data_size, crc32, reserved      │
//! ├─ Sparse Offset Index ───────────────────────────────┤
//! │  [index_entry_count × u32]                          │
//! │  entry[i] = byte_pos of record at start_offset+i*64 │
//! ├─ Record Data ───────────────────────────────────────┤
//! │  [len:u32 BE][payload] ... (arena wire format)      │
//! └─────────────────────────────────────────────────────┘
//! ```

use bytes::Bytes;
use common::types::StreamId;

use crate::extent::Extent;

/// Magic bytes identifying an S3 extent file: "SEXT" in ASCII.
pub const S3_EXTENT_MAGIC: u32 = 0x53455854;

/// Current S3 extent file format version.
pub const S3_EXTENT_VERSION: u16 = 1;

/// Fixed header size in bytes.
pub const S3_EXTENT_HEADER_SIZE: usize = 64;

/// Default sparse index interval: one entry per N records.
pub const S3_INDEX_INTERVAL: u32 = 64;

/// Parsed S3 extent file header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ExtentHeader {
    pub magic: u32,
    pub version: u16,
    pub flags: u16,
    pub stream_id: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub record_count: u32,
    pub index_interval: u32,
    pub index_entry_count: u32,
    pub data_size: u32,
    pub crc32: u32,
}

impl S3ExtentHeader {
    /// Encode this header into a fixed 64-byte array.
    pub fn encode(&self) -> [u8; S3_EXTENT_HEADER_SIZE] {
        let mut buf = [0u8; S3_EXTENT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_be_bytes());
        buf[4..6].copy_from_slice(&self.version.to_be_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_be_bytes());
        buf[8..16].copy_from_slice(&self.stream_id.to_be_bytes());
        buf[16..24].copy_from_slice(&self.start_offset.to_be_bytes());
        buf[24..32].copy_from_slice(&self.end_offset.to_be_bytes());
        buf[32..36].copy_from_slice(&self.record_count.to_be_bytes());
        buf[36..40].copy_from_slice(&self.index_interval.to_be_bytes());
        buf[40..44].copy_from_slice(&self.index_entry_count.to_be_bytes());
        buf[44..48].copy_from_slice(&self.data_size.to_be_bytes());
        buf[48..52].copy_from_slice(&self.crc32.to_be_bytes());
        // bytes 52..64 are reserved (already zeroed)
        buf
    }

    /// Decode a header from a byte slice (must be at least 64 bytes).
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        if buf.len() < S3_EXTENT_HEADER_SIZE {
            return Err(CodecError::HeaderTooShort(buf.len()));
        }
        let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != S3_EXTENT_MAGIC {
            return Err(CodecError::BadMagic(magic));
        }
        let version = u16::from_be_bytes([buf[4], buf[5]]);
        if version != S3_EXTENT_VERSION {
            return Err(CodecError::UnsupportedVersion(version));
        }
        let flags = u16::from_be_bytes([buf[6], buf[7]]);
        let stream_id = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        let start_offset = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        let end_offset = u64::from_be_bytes(buf[24..32].try_into().unwrap());
        let record_count = u32::from_be_bytes(buf[32..36].try_into().unwrap());
        let index_interval = u32::from_be_bytes(buf[36..40].try_into().unwrap());
        let index_entry_count = u32::from_be_bytes(buf[40..44].try_into().unwrap());
        let data_size = u32::from_be_bytes(buf[44..48].try_into().unwrap());
        let crc32 = u32::from_be_bytes(buf[48..52].try_into().unwrap());

        Ok(Self {
            magic,
            version,
            flags,
            stream_id,
            start_offset,
            end_offset,
            record_count,
            index_interval,
            index_entry_count,
            data_size,
            crc32,
        })
    }

    /// Byte offset where the sparse index starts (immediately after header).
    pub fn index_offset(&self) -> usize {
        S3_EXTENT_HEADER_SIZE
    }

    /// Byte offset where record data starts (after header + index).
    pub fn data_offset(&self) -> usize {
        S3_EXTENT_HEADER_SIZE + self.index_entry_count as usize * 4
    }
}

/// Build the S3 object key for an extent.
///
/// Format: `{namespace}/data/{stream_id}/{start_offset}_{end_offset}.dat`
pub fn s3_key(namespace: &str, stream_id: StreamId, start_offset: u64, end_offset: u64) -> String {
    format!(
        "{}/data/{}/{}_{}.dat",
        namespace, stream_id.0, start_offset, end_offset,
    )
}

/// Encode a sealed extent into the S3 file format.
///
/// The extent must be sealed. Returns the complete file bytes
/// (header + sparse index + record data).
pub fn encode_extent(stream_id: StreamId, extent: &Extent) -> Vec<u8> {
    let data: Bytes = extent.committed_data();
    let data_size = data.len() as u32;
    let record_count = extent.message_count() as u32;
    let start_offset = extent.start_offset.0;
    let end_offset = start_offset + record_count as u64;

    // Build sparse index.
    let index_entry_count = if record_count == 0 {
        0
    } else {
        (record_count + S3_INDEX_INTERVAL - 1) / S3_INDEX_INTERVAL
    };

    let index_size = index_entry_count as usize * 4;
    let mut index_bytes = Vec::with_capacity(index_size);
    for i in 0..index_entry_count {
        let seq = (i * S3_INDEX_INTERVAL) as u64;
        let byte_pos = extent.index_lookup(seq).unwrap_or(0) as u32;
        index_bytes.extend_from_slice(&byte_pos.to_be_bytes());
    }

    // CRC32 over index + data.
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&index_bytes);
    hasher.update(&data);
    let crc32 = hasher.finalize();

    let header = S3ExtentHeader {
        magic: S3_EXTENT_MAGIC,
        version: S3_EXTENT_VERSION,
        flags: 0,
        stream_id: stream_id.0,
        start_offset,
        end_offset,
        record_count,
        index_interval: S3_INDEX_INTERVAL,
        index_entry_count,
        data_size,
        crc32,
    };

    let total_size = S3_EXTENT_HEADER_SIZE + index_size + data.len();
    let mut out = Vec::with_capacity(total_size);
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(&index_bytes);
    out.extend_from_slice(&data);
    out
}

/// Errors from S3 extent codec operations.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("header too short: {0} bytes (need {S3_EXTENT_HEADER_SIZE})")]
    HeaderTooShort(usize),

    #[error("bad magic: {0:#010x} (expected {S3_EXTENT_MAGIC:#010x})")]
    BadMagic(u32),

    #[error("unsupported version: {0} (expected {S3_EXTENT_VERSION})")]
    UnsupportedVersion(u16),
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::types::{Epoch, ExtentId, Offset};

    /// Helper: create a sealed extent with N records of given payloads.
    fn sealed_extent(payloads: &[&[u8]]) -> Extent {
        let extent = Extent::with_capacity(
            ExtentId(1),
            Offset(0),
            1024 * 1024, // 1 MiB
            Epoch(0),
        );
        for payload in payloads {
            extent
                .append(Bytes::copy_from_slice(payload))
                .expect("append should succeed");
        }
        extent.seal(None);
        extent
    }

    /// Helper: create a sealed extent with start_offset.
    fn sealed_extent_at(start_offset: u64, payloads: &[&[u8]]) -> Extent {
        let extent =
            Extent::with_capacity(ExtentId(1), Offset(start_offset), 1024 * 1024, Epoch(0));
        for payload in payloads {
            extent
                .append(Bytes::copy_from_slice(payload))
                .expect("append should succeed");
        }
        extent.seal(None);
        extent
    }

    #[test]
    fn header_encode_decode_round_trip() {
        let header = S3ExtentHeader {
            magic: S3_EXTENT_MAGIC,
            version: S3_EXTENT_VERSION,
            flags: 0,
            stream_id: 42,
            start_offset: 1000,
            end_offset: 2000,
            record_count: 1000,
            index_interval: S3_INDEX_INTERVAL,
            index_entry_count: 16,
            data_size: 50000,
            crc32: 0xDEADBEEF,
        };

        let encoded = header.encode();
        assert_eq!(encoded.len(), S3_EXTENT_HEADER_SIZE);

        let decoded = S3ExtentHeader::decode(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn header_decode_bad_magic() {
        let mut buf = [0u8; S3_EXTENT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&0xBAD_u32.to_be_bytes());
        assert!(matches!(
            S3ExtentHeader::decode(&buf),
            Err(CodecError::BadMagic(0xBAD))
        ));
    }

    #[test]
    fn header_decode_too_short() {
        let buf = [0u8; 32];
        assert!(matches!(
            S3ExtentHeader::decode(&buf),
            Err(CodecError::HeaderTooShort(32))
        ));
    }

    #[test]
    fn s3_key_format() {
        let key = s3_key("default", StreamId(42), 1000, 2000);
        assert_eq!(key, "default/data/42/1000_2000.dat");
    }

    #[test]
    fn encode_empty_extent() {
        let extent = sealed_extent(&[]);
        let encoded = encode_extent(StreamId(1), &extent);

        // Should be just the header (no index, no data).
        assert_eq!(encoded.len(), S3_EXTENT_HEADER_SIZE);

        let header = S3ExtentHeader::decode(&encoded).unwrap();
        assert_eq!(header.record_count, 0);
        assert_eq!(header.index_entry_count, 0);
        assert_eq!(header.data_size, 0);
        assert_eq!(header.start_offset, 0);
        assert_eq!(header.end_offset, 0);
    }

    #[test]
    fn encode_small_extent_no_sparse_gap() {
        // 3 records — fewer than index_interval (64), so 1 index entry.
        let extent = sealed_extent(&[b"aaa", b"bbb", b"ccc"]);
        let encoded = encode_extent(StreamId(5), &extent);

        let header = S3ExtentHeader::decode(&encoded).unwrap();
        assert_eq!(header.stream_id, 5);
        assert_eq!(header.record_count, 3);
        assert_eq!(header.index_entry_count, 1); // ceil(3/64) = 1
        assert_eq!(header.start_offset, 0);
        assert_eq!(header.end_offset, 3);
        assert_eq!(header.index_interval, S3_INDEX_INTERVAL);

        // Verify data_size: 3 records × (4 byte len + 3 byte payload) = 21
        assert_eq!(header.data_size, 21);

        // Verify total size: 64 header + 4 index + 21 data = 89
        assert_eq!(encoded.len(), 64 + 4 + 21);

        // First index entry should be byte_pos 0 (first record starts at arena offset 0).
        let idx0 = u32::from_be_bytes(encoded[64..68].try_into().unwrap());
        assert_eq!(idx0, 0);

        // Verify CRC32 over index + data.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&encoded[64..68]); // index
        hasher.update(&encoded[68..]); // data
        assert_eq!(header.crc32, hasher.finalize());
    }

    #[test]
    fn encode_extent_with_multiple_index_entries() {
        // Create 200 records — should yield ceil(200/64) = 4 index entries.
        let payloads: Vec<Vec<u8>> = (0..200)
            .map(|i| format!("record-{i:04}").into_bytes())
            .collect();
        let payload_refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
        let extent = sealed_extent(&payload_refs);
        let encoded = encode_extent(StreamId(10), &extent);

        let header = S3ExtentHeader::decode(&encoded).unwrap();
        assert_eq!(header.record_count, 200);
        assert_eq!(header.index_entry_count, 4); // ceil(200/64) = 4
        assert_eq!(header.end_offset, 200);

        // Verify index entries point to correct byte positions.
        let data_start = header.data_offset();
        for i in 0..4u32 {
            let seq = i * S3_INDEX_INTERVAL;
            let idx_offset = S3_EXTENT_HEADER_SIZE + i as usize * 4;
            let byte_pos =
                u32::from_be_bytes(encoded[idx_offset..idx_offset + 4].try_into().unwrap());

            // The byte_pos should match what we can verify from the extent.
            let expected = extent.index_lookup(seq as u64).unwrap() as u32;
            assert_eq!(byte_pos, expected, "index entry {i} mismatch");

            // Walk from byte_pos in the data section to verify the record is there.
            let abs_pos = data_start + byte_pos as usize;
            let len =
                u32::from_be_bytes(encoded[abs_pos..abs_pos + 4].try_into().unwrap()) as usize;
            let record = &encoded[abs_pos + 4..abs_pos + 4 + len];
            let expected_payload = format!("record-{seq:04}");
            assert_eq!(record, expected_payload.as_bytes(), "record at seq {seq}");
        }

        // Verify CRC32.
        let index_and_data = &encoded[S3_EXTENT_HEADER_SIZE..];
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(index_and_data);
        assert_eq!(header.crc32, hasher.finalize());
    }

    #[test]
    fn encode_extent_with_nonzero_start_offset() {
        let extent = sealed_extent_at(500, &[b"hello", b"world"]);
        let encoded = encode_extent(StreamId(7), &extent);

        let header = S3ExtentHeader::decode(&encoded).unwrap();
        assert_eq!(header.start_offset, 500);
        assert_eq!(header.end_offset, 502);
        assert_eq!(header.record_count, 2);
    }

    #[test]
    fn data_offset_and_index_offset() {
        let header = S3ExtentHeader {
            magic: S3_EXTENT_MAGIC,
            version: S3_EXTENT_VERSION,
            flags: 0,
            stream_id: 1,
            start_offset: 0,
            end_offset: 256,
            record_count: 256,
            index_interval: S3_INDEX_INTERVAL,
            index_entry_count: 4,
            data_size: 10000,
            crc32: 0,
        };

        assert_eq!(header.index_offset(), 64);
        assert_eq!(header.data_offset(), 64 + 4 * 4); // 64 + 16 = 80
    }
}
