//! S3 extent file codec: encode sealed extents for S3 upload and decode headers
//! for S3 range-read.
//!
//! ## File Layout (v2, chunk-compressed)
//!
//! ```text
//! ┌─ Header (fixed 64 bytes) ──────────────────────────────┐
//! │  magic, version, flags, stream_id, start_offset,        │
//! │  end_offset, record_count, index_interval,              │
//! │  chunk_count, data_size, crc32, compression, reserved   │
//! ├─ Chunk Index ───────────────────────────────────────────┤
//! │  [chunk_count × u32]                                    │
//! │  entry[i] = byte offset of chunk[i] within data section │
//! ├─ Data (compressed chunks) ──────────────────────────────┤
//! │  chunk[0]: compress(records[0..64])                     │
//! │  chunk[1]: compress(records[64..128])                   │
//! │  ...                                                    │
//! │  Each chunk is independently (de)compressible.          │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! When compression=None, each chunk is the raw arena wire-format bytes
//! (`[len:u32 BE][payload]...`), identical to v1.

use bytes::Bytes;
use common::types::StreamId;

use crate::extent::Extent;

/// Magic bytes identifying an S3 extent file: "SEXT" in ASCII.
pub const S3_EXTENT_MAGIC: u32 = 0x53455854;

/// Current S3 extent file format version.
pub const S3_EXTENT_VERSION: u16 = 2;

/// Fixed header size in bytes.
pub const S3_EXTENT_HEADER_SIZE: usize = 64;

/// Default sparse index / chunk interval: one entry per N records.
pub const S3_INDEX_INTERVAL: u32 = 64;

// ── Compression ─────────────────────────────────────────────────────────────

/// Compression algorithm for S3 extent chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Zstd = 1,
    Lz4 = 2,
}

impl Compression {
    /// Parse from a u8 tag.
    pub fn from_u8(v: u8) -> Result<Self, CodecError> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Zstd),
            2 => Ok(Self::Lz4),
            _ => Err(UnsupportedCompressionSnafu { tag: v }.build()),
        }
    }

    /// Parse from a config string ("none", "zstd", "lz4").
    pub fn from_config(s: &str) -> Result<Self, CodecError> {
        match s.to_ascii_lowercase().as_str() {
            "none" | "" => Ok(Self::None),
            "zstd" => Ok(Self::Zstd),
            "lz4" => Ok(Self::Lz4),
            _ => Err(UnknownCompressionNameSnafu {
                name: s.to_string(),
            }
            .build()),
        }
    }

    fn compress(&self, data: &[u8]) -> Vec<u8> {
        match self {
            Compression::None => data.to_vec(),
            Compression::Zstd => zstd::bulk::compress(data, 3).expect("zstd compress"),
            Compression::Lz4 => lz4::block::compress(data, None, false).expect("lz4 compress"),
        }
    }

    pub fn decompress(
        &self,
        data: &[u8],
        max_decompressed_size: usize,
    ) -> Result<Vec<u8>, CodecError> {
        match self {
            Compression::None => Ok(data.to_vec()),
            Compression::Zstd => zstd::bulk::decompress(data, max_decompressed_size).map_err(|e| {
                DecompressFailedSnafu {
                    message: format!("zstd: {e}"),
                }
                .build()
            }),
            Compression::Lz4 => lz4::block::decompress(data, Some(max_decompressed_size as i32))
                .map_err(|e| {
                    DecompressFailedSnafu {
                        message: format!("lz4: {e}"),
                    }
                    .build()
                }),
        }
    }
}

// ── Header ──────────────────────────────────────────────────────────────────

/// Parsed S3 extent file header (64 bytes).
///
/// Layout (all big-endian):
/// ```text
/// offset  field              type
///  0      magic              u32
///  4      version            u16
///  6      flags              u16
///  8      stream_id          u64
/// 16      start_offset       u64
/// 24      end_offset         u64
/// 32      record_count       u32
/// 36      index_interval     u32
/// 40      chunk_count        u32
/// 44      data_size          u32   (total compressed data bytes)
/// 48      crc32              u32   (over index + data)
/// 52      compression        u8    (0=none, 1=zstd, 2=lz4)
/// 53      _reserved          [u8; 11]
/// ```
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
    /// Number of chunks (each chunk holds up to `index_interval` records).
    /// Equal to `ceil(record_count / index_interval)`.
    pub chunk_count: u32,
    /// Total size of the data section (sum of all compressed chunk bytes).
    pub data_size: u32,
    /// CRC32 over chunk index + data section.
    pub crc32: u32,
    /// Compression algorithm used for each chunk.
    pub compression: Compression,
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
        buf[40..44].copy_from_slice(&self.chunk_count.to_be_bytes());
        buf[44..48].copy_from_slice(&self.data_size.to_be_bytes());
        buf[48..52].copy_from_slice(&self.crc32.to_be_bytes());
        buf[52] = self.compression as u8;
        // bytes 53..64 are reserved (already zeroed)
        buf
    }

    /// Decode a header from a byte slice (must be at least 64 bytes).
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        if buf.len() < S3_EXTENT_HEADER_SIZE {
            return Err(HeaderTooShortSnafu { size: buf.len() }.build());
        }
        let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != S3_EXTENT_MAGIC {
            return Err(BadMagicSnafu { magic: magic }.build());
        }
        let version = u16::from_be_bytes([buf[4], buf[5]]);
        if version != S3_EXTENT_VERSION {
            return Err(UnsupportedVersionSnafu { version: version }.build());
        }
        let flags = u16::from_be_bytes([buf[6], buf[7]]);
        let stream_id = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        let start_offset = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        let end_offset = u64::from_be_bytes(buf[24..32].try_into().unwrap());
        let record_count = u32::from_be_bytes(buf[32..36].try_into().unwrap());
        let index_interval = u32::from_be_bytes(buf[36..40].try_into().unwrap());
        let chunk_count = u32::from_be_bytes(buf[40..44].try_into().unwrap());
        let data_size = u32::from_be_bytes(buf[44..48].try_into().unwrap());
        let crc32 = u32::from_be_bytes(buf[48..52].try_into().unwrap());
        let compression = Compression::from_u8(buf[52])?;

        Ok(Self {
            magic,
            version,
            flags,
            stream_id,
            start_offset,
            end_offset,
            record_count,
            index_interval,
            chunk_count,
            data_size,
            crc32,
            compression,
        })
    }

    /// Byte offset where the chunk index starts (immediately after header).
    pub fn index_offset(&self) -> usize {
        S3_EXTENT_HEADER_SIZE
    }

    /// Byte offset where the data section starts (after header + index).
    pub fn data_offset(&self) -> usize {
        S3_EXTENT_HEADER_SIZE + self.chunk_count as usize * 4
    }
}

// ── Key ─────────────────────────────────────────────────────────────────────

/// Build the S3 object key for an extent.
///
/// Format: `{namespace}/data/{stream_id}/{start_offset}_{end_offset}.dat`
pub fn s3_key(namespace: &str, stream_id: StreamId, start_offset: u64, end_offset: u64) -> String {
    format!(
        "{}/data/{}/{}_{}.dat",
        namespace, stream_id.0, start_offset, end_offset,
    )
}

// ── Encode ──────────────────────────────────────────────────────────────────

/// Encode a sealed extent into the S3 file format with chunk-based compression.
///
/// Records are grouped into chunks of `S3_INDEX_INTERVAL` (64) records each.
/// Each chunk is compressed independently with the given `compression` algorithm.
/// The chunk index stores the byte offset of each compressed chunk within the
/// data section.
///
/// The extent must be sealed. Returns the complete file bytes
/// (header + chunk index + compressed data).
pub fn encode_extent(stream_id: StreamId, extent: &Extent, compression: Compression) -> Vec<u8> {
    let data: Bytes = extent.committed_data();
    let record_count = extent.message_count() as u32;
    let start_offset = extent.start_offset.0;
    let end_offset = start_offset + record_count as u64;

    // Number of chunks: ceil(record_count / interval).
    let chunk_count = if record_count == 0 {
        0
    } else {
        (record_count + S3_INDEX_INTERVAL - 1) / S3_INDEX_INTERVAL
    };

    // Build chunks: split committed data into groups of 64 records, compress each.
    let mut chunk_offsets: Vec<u32> = Vec::with_capacity(chunk_count as usize);
    let mut compressed_data: Vec<u8> = Vec::new();

    for i in 0..chunk_count {
        let chunk_start_seq = (i * S3_INDEX_INTERVAL) as u64;
        let chunk_end_seq =
            std::cmp::min(((i + 1) * S3_INDEX_INTERVAL) as u64, record_count as u64);

        // Determine byte range for this chunk's records in the raw arena data.
        let byte_start = if chunk_start_seq == 0 {
            0usize
        } else {
            extent.index_lookup(chunk_start_seq).unwrap_or(0) as usize
        };

        let byte_end = if chunk_end_seq >= record_count as u64 {
            // Last chunk: goes to the end of committed data.
            data.len()
        } else {
            extent
                .index_lookup(chunk_end_seq)
                .unwrap_or(data.len() as u64) as usize
        };

        let raw_chunk = &data[byte_start..byte_end];

        // Record offset of this compressed chunk in the data section.
        chunk_offsets.push(compressed_data.len() as u32);

        // Compress and append.
        let compressed_chunk = compression.compress(raw_chunk);
        compressed_data.extend_from_slice(&compressed_chunk);
    }

    let data_size = compressed_data.len() as u32;

    // Build chunk index bytes.
    let index_size = chunk_count as usize * 4;
    let mut index_bytes = Vec::with_capacity(index_size);
    for offset in &chunk_offsets {
        index_bytes.extend_from_slice(&offset.to_be_bytes());
    }

    // CRC32 over index + compressed data.
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&index_bytes);
    hasher.update(&compressed_data);
    let crc32 = hasher.finalize();

    let header = S3ExtentHeader {
        magic: S3_EXTENT_MAGIC,
        version: S3_EXTENT_VERSION,
        flags: 0,
        stream_id: stream_id.0 as u64,
        start_offset,
        end_offset,
        record_count,
        index_interval: S3_INDEX_INTERVAL,
        chunk_count,
        data_size,
        crc32,
        compression,
    };

    let total_size = S3_EXTENT_HEADER_SIZE + index_size + compressed_data.len();
    let mut out = Vec::with_capacity(total_size);
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(&index_bytes);
    out.extend_from_slice(&compressed_data);
    out
}

/// Encode a sealed extent for S3 upload, respecting an authoritative end_offset.
///
/// During DR flush, a secondary may hold more data than the quorum-committed
/// point. This function encodes only records in `[extent.start_offset, end_offset)`,
/// using the caller-supplied `end_offset` for both the S3 header and record
/// truncation. The S3 key must also use this `end_offset`.
pub fn encode_extent_range(
    stream_id: StreamId,
    extent: &Extent,
    compression: Compression,
    end_offset: u64,
) -> Vec<u8> {
    let start_offset = extent.start_offset.0;
    let record_count = if end_offset > start_offset {
        (end_offset - start_offset) as u32
    } else {
        0
    };

    // Determine the byte range for the requested records.
    let data: Bytes = if record_count == 0 {
        Bytes::new()
    } else {
        let full_data = extent.committed_data();
        let local_end_seq = record_count as u64;
        let byte_end = extent
            .index_lookup(local_end_seq)
            .map(|bp| bp as usize)
            .unwrap_or(full_data.len());
        full_data.slice(0..byte_end)
    };

    // Number of chunks: ceil(record_count / interval).
    let chunk_count = if record_count == 0 {
        0
    } else {
        (record_count + S3_INDEX_INTERVAL - 1) / S3_INDEX_INTERVAL
    };

    // Build chunks: split data into groups of 64 records, compress each.
    let mut chunk_offsets: Vec<u32> = Vec::with_capacity(chunk_count as usize);
    let mut compressed_data: Vec<u8> = Vec::new();

    for i in 0..chunk_count {
        let chunk_start_seq = (i * S3_INDEX_INTERVAL) as u64;
        let chunk_end_seq =
            std::cmp::min(((i + 1) * S3_INDEX_INTERVAL) as u64, record_count as u64);

        let byte_start = if chunk_start_seq == 0 {
            0usize
        } else {
            extent.index_lookup(chunk_start_seq).unwrap_or(0) as usize
        };

        let byte_end = if chunk_end_seq >= record_count as u64 {
            data.len()
        } else {
            extent
                .index_lookup(chunk_end_seq)
                .unwrap_or(data.len() as u64) as usize
        };

        let raw_chunk = &data[byte_start..byte_end];
        chunk_offsets.push(compressed_data.len() as u32);
        let compressed_chunk = compression.compress(raw_chunk);
        compressed_data.extend_from_slice(&compressed_chunk);
    }

    let data_size = compressed_data.len() as u32;

    let index_size = chunk_count as usize * 4;
    let mut index_bytes = Vec::with_capacity(index_size);
    for offset in &chunk_offsets {
        index_bytes.extend_from_slice(&offset.to_be_bytes());
    }

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&index_bytes);
    hasher.update(&compressed_data);
    let crc32 = hasher.finalize();

    let header = S3ExtentHeader {
        magic: S3_EXTENT_MAGIC,
        version: S3_EXTENT_VERSION,
        flags: 0,
        stream_id: stream_id.0 as u64,
        start_offset,
        end_offset,
        record_count,
        index_interval: S3_INDEX_INTERVAL,
        chunk_count,
        data_size,
        crc32,
        compression,
    };

    let total_size = S3_EXTENT_HEADER_SIZE + index_size + compressed_data.len();
    let mut out = Vec::with_capacity(total_size);
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(&index_bytes);
    out.extend_from_slice(&compressed_data);
    out
}

// ── Errors ──────────────────────────────────────────────────────────────────

/// Errors from S3 extent codec operations.
#[derive(snafu::Snafu)]
#[snafu(visibility(pub))]
#[snafu_virtstack::stack_trace_debug]
pub enum CodecError {
    #[snafu(display("header too short: {size} bytes (need {S3_EXTENT_HEADER_SIZE})"))]
    HeaderTooShort {
        size: usize,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    #[snafu(display("bad magic: {magic:#010x} (expected {S3_EXTENT_MAGIC:#010x})"))]
    BadMagic {
        magic: u32,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    #[snafu(display("unsupported version: {version} (expected {S3_EXTENT_VERSION})"))]
    UnsupportedVersion {
        version: u16,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    #[snafu(display("unsupported compression tag: {tag}"))]
    UnsupportedCompression {
        tag: u8,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    #[snafu(display("unknown compression name: {name:?} (expected none/zstd/lz4)"))]
    UnknownCompressionName {
        name: String,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    #[snafu(display("decompression failed: {message}"))]
    DecompressFailed {
        message: String,
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

// ── Tests ───────────────────────────────────────────────────────────────────

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

    /// Verify CRC32 over index + data in encoded bytes.
    fn verify_crc32(encoded: &[u8], header: &S3ExtentHeader) {
        let index_and_data = &encoded[S3_EXTENT_HEADER_SIZE..];
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(index_and_data);
        assert_eq!(header.crc32, hasher.finalize(), "CRC32 mismatch");
    }

    /// Decompress a single chunk from encoded bytes.
    fn decompress_chunk(encoded: &[u8], header: &S3ExtentHeader, chunk_idx: u32) -> Vec<u8> {
        let data_start = header.data_offset();
        let idx_base = S3_EXTENT_HEADER_SIZE + chunk_idx as usize * 4;
        let chunk_byte_start =
            u32::from_be_bytes(encoded[idx_base..idx_base + 4].try_into().unwrap()) as usize;
        let chunk_byte_end = if chunk_idx + 1 < header.chunk_count {
            let next_base = idx_base + 4;
            u32::from_be_bytes(encoded[next_base..next_base + 4].try_into().unwrap()) as usize
        } else {
            header.data_size as usize
        };
        let compressed = &encoded[data_start + chunk_byte_start..data_start + chunk_byte_end];
        header
            .compression
            .decompress(compressed, 64 * 1024 * 1024)
            .expect("decompress should succeed")
    }

    // ── Header tests ────────────────────────────────────────────────────────

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
            chunk_count: 16,
            data_size: 50000,
            crc32: 0xDEADBEEF,
            compression: Compression::None,
        };

        let encoded = header.encode();
        assert_eq!(encoded.len(), S3_EXTENT_HEADER_SIZE);

        let decoded = S3ExtentHeader::decode(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn header_compression_field_round_trip() {
        for comp in [Compression::None, Compression::Zstd, Compression::Lz4] {
            let header = S3ExtentHeader {
                magic: S3_EXTENT_MAGIC,
                version: S3_EXTENT_VERSION,
                flags: 0,
                stream_id: 1,
                start_offset: 0,
                end_offset: 100,
                record_count: 100,
                index_interval: S3_INDEX_INTERVAL,
                chunk_count: 2,
                data_size: 5000,
                crc32: 0,
                compression: comp,
            };
            let buf = header.encode();
            let decoded = S3ExtentHeader::decode(&buf).unwrap();
            assert_eq!(
                decoded.compression, comp,
                "compression round-trip for {comp:?}"
            );
        }
    }

    #[test]
    fn header_decode_bad_magic() {
        let mut buf = [0u8; S3_EXTENT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&0xBAD_u32.to_be_bytes());
        assert!(matches!(
            S3ExtentHeader::decode(&buf),
            Err(CodecError::BadMagic { .. })
        ));
    }

    #[test]
    fn header_decode_too_short() {
        let buf = [0u8; 32];
        assert!(matches!(
            S3ExtentHeader::decode(&buf),
            Err(CodecError::HeaderTooShort { .. })
        ));
    }

    #[test]
    fn s3_key_format() {
        let key = s3_key("default", StreamId(42), 1000, 2000);
        assert_eq!(key, "default/data/42/1000_2000.dat");
    }

    // ── Compression::None tests ─────────────────────────────────────────────

    #[test]
    fn encode_empty_extent_no_compression() {
        let extent = sealed_extent(&[]);
        let encoded = encode_extent(StreamId(1), &extent, Compression::None);

        // Should be just the header (no index, no data).
        assert_eq!(encoded.len(), S3_EXTENT_HEADER_SIZE);

        let header = S3ExtentHeader::decode(&encoded).unwrap();
        assert_eq!(header.record_count, 0);
        assert_eq!(header.chunk_count, 0);
        assert_eq!(header.data_size, 0);
        assert_eq!(header.start_offset, 0);
        assert_eq!(header.end_offset, 0);
        assert_eq!(header.compression, Compression::None);
    }

    #[test]
    fn encode_small_extent_no_compression() {
        // 3 records — fewer than index_interval (64), so 1 chunk.
        let extent = sealed_extent(&[b"aaa", b"bbb", b"ccc"]);
        let encoded = encode_extent(StreamId(5), &extent, Compression::None);

        let header = S3ExtentHeader::decode(&encoded).unwrap();
        assert_eq!(header.stream_id, 5);
        assert_eq!(header.record_count, 3);
        assert_eq!(header.chunk_count, 1); // ceil(3/64) = 1
        assert_eq!(header.start_offset, 0);
        assert_eq!(header.end_offset, 3);
        assert_eq!(header.compression, Compression::None);

        // With no compression, data_size == raw data size.
        // 3 records x (4 byte len + 3 byte payload) = 21
        assert_eq!(header.data_size, 21);

        // Total: 64 header + 4 index + 21 data = 89
        assert_eq!(encoded.len(), 64 + 4 + 21);

        // First chunk index entry should be 0.
        let idx0 = u32::from_be_bytes(encoded[64..68].try_into().unwrap());
        assert_eq!(idx0, 0);

        verify_crc32(&encoded, &header);

        // Decompress chunk 0 and verify it matches raw data.
        let decompressed = decompress_chunk(&encoded, &header, 0);
        assert_eq!(decompressed.len(), 21);
    }

    #[test]
    fn encode_extent_multiple_chunks_no_compression() {
        // 200 records — ceil(200/64) = 4 chunks.
        let payloads: Vec<Vec<u8>> = (0..200)
            .map(|i| format!("record-{i:04}").into_bytes())
            .collect();
        let payload_refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
        let extent = sealed_extent(&payload_refs);
        let encoded = encode_extent(StreamId(10), &extent, Compression::None);

        let header = S3ExtentHeader::decode(&encoded).unwrap();
        assert_eq!(header.record_count, 200);
        assert_eq!(header.chunk_count, 4);
        assert_eq!(header.end_offset, 200);
        assert_eq!(header.compression, Compression::None);

        verify_crc32(&encoded, &header);

        // Verify each chunk decompresses to the correct records.
        for chunk_idx in 0..4u32 {
            let decompressed = decompress_chunk(&encoded, &header, chunk_idx);
            let first_seq = chunk_idx * S3_INDEX_INTERVAL;

            // Read the first record from the decompressed chunk.
            let len = u32::from_be_bytes(decompressed[0..4].try_into().unwrap()) as usize;
            let record = &decompressed[4..4 + len];
            let expected = format!("record-{first_seq:04}");
            assert_eq!(
                record,
                expected.as_bytes(),
                "chunk {chunk_idx} first record"
            );
        }
    }

    #[test]
    fn encode_extent_with_nonzero_start_offset() {
        let extent = sealed_extent_at(500, &[b"hello", b"world"]);
        let encoded = encode_extent(StreamId(7), &extent, Compression::None);

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
            chunk_count: 4,
            data_size: 10000,
            crc32: 0,
            compression: Compression::None,
        };

        assert_eq!(header.index_offset(), 64);
        assert_eq!(header.data_offset(), 64 + 4 * 4); // 64 + 16 = 80
    }

    // ── Compression tests ───────────────────────────────────────────────────

    #[test]
    fn encode_extent_zstd_compression() {
        let payloads: Vec<Vec<u8>> = (0..200)
            .map(|i| format!("record-{i:04}-padding-data-for-compression").into_bytes())
            .collect();
        let payload_refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
        let extent = sealed_extent(&payload_refs);

        let encoded = encode_extent(StreamId(20), &extent, Compression::Zstd);
        let header = S3ExtentHeader::decode(&encoded).unwrap();

        assert_eq!(header.record_count, 200);
        assert_eq!(header.chunk_count, 4);
        assert_eq!(header.compression, Compression::Zstd);

        // Compressed data should be smaller than raw.
        let raw_encoded = encode_extent(StreamId(20), &extent, Compression::None);
        let raw_header = S3ExtentHeader::decode(&raw_encoded).unwrap();
        assert!(
            header.data_size < raw_header.data_size,
            "zstd should compress: {} vs {} raw",
            header.data_size,
            raw_header.data_size,
        );

        verify_crc32(&encoded, &header);

        // Decompress each chunk and verify first record.
        for chunk_idx in 0..4u32 {
            let decompressed = decompress_chunk(&encoded, &header, chunk_idx);
            let first_seq = chunk_idx * S3_INDEX_INTERVAL;
            let len = u32::from_be_bytes(decompressed[0..4].try_into().unwrap()) as usize;
            let record = &decompressed[4..4 + len];
            let expected = format!("record-{first_seq:04}-padding-data-for-compression");
            assert_eq!(record, expected.as_bytes(), "zstd chunk {chunk_idx}");
        }
    }

    #[test]
    fn encode_extent_lz4_compression() {
        let payloads: Vec<Vec<u8>> = (0..200)
            .map(|i| format!("record-{i:04}-padding-data-for-compression").into_bytes())
            .collect();
        let payload_refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
        let extent = sealed_extent(&payload_refs);

        let encoded = encode_extent(StreamId(30), &extent, Compression::Lz4);
        let header = S3ExtentHeader::decode(&encoded).unwrap();

        assert_eq!(header.record_count, 200);
        assert_eq!(header.chunk_count, 4);
        assert_eq!(header.compression, Compression::Lz4);

        // Compressed should be smaller than raw.
        let raw_encoded = encode_extent(StreamId(30), &extent, Compression::None);
        let raw_header = S3ExtentHeader::decode(&raw_encoded).unwrap();
        assert!(
            header.data_size < raw_header.data_size,
            "lz4 should compress: {} vs {} raw",
            header.data_size,
            raw_header.data_size,
        );

        verify_crc32(&encoded, &header);

        // Decompress each chunk and verify first record.
        for chunk_idx in 0..4u32 {
            let decompressed = decompress_chunk(&encoded, &header, chunk_idx);
            let first_seq = chunk_idx * S3_INDEX_INTERVAL;
            let len = u32::from_be_bytes(decompressed[0..4].try_into().unwrap()) as usize;
            let record = &decompressed[4..4 + len];
            let expected = format!("record-{first_seq:04}-padding-data-for-compression");
            assert_eq!(record, expected.as_bytes(), "lz4 chunk {chunk_idx}");
        }
    }

    #[test]
    fn encode_small_extent_zstd() {
        // 3 records, single chunk — should still compress/decompress correctly.
        let extent = sealed_extent(&[b"aaa", b"bbb", b"ccc"]);
        let encoded = encode_extent(StreamId(5), &extent, Compression::Zstd);

        let header = S3ExtentHeader::decode(&encoded).unwrap();
        assert_eq!(header.record_count, 3);
        assert_eq!(header.chunk_count, 1);
        assert_eq!(header.compression, Compression::Zstd);

        verify_crc32(&encoded, &header);

        let decompressed = decompress_chunk(&encoded, &header, 0);
        // Raw: 3 records x (4 + 3) = 21 bytes.
        assert_eq!(decompressed.len(), 21);
    }

    #[test]
    fn compression_from_config() {
        assert_eq!(Compression::from_config("none").unwrap(), Compression::None);
        assert_eq!(Compression::from_config("").unwrap(), Compression::None);
        assert_eq!(Compression::from_config("zstd").unwrap(), Compression::Zstd);
        assert_eq!(Compression::from_config("ZSTD").unwrap(), Compression::Zstd);
        assert_eq!(Compression::from_config("lz4").unwrap(), Compression::Lz4);
        assert_eq!(Compression::from_config("LZ4").unwrap(), Compression::Lz4);
        assert!(Compression::from_config("brotli").is_err());
    }

    #[test]
    fn compression_from_u8_round_trip() {
        for val in 0..=2u8 {
            let comp = Compression::from_u8(val).unwrap();
            assert_eq!(comp as u8, val);
        }
        assert!(Compression::from_u8(3).is_err());
        assert!(Compression::from_u8(255).is_err());
    }
}
