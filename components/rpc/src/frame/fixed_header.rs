use common::types::Opcode;

/// Fixed header fields present in every frame on the wire.
///
/// During encoding, `flags` is computed from `Option` fields in the variable
/// header (eliminating stale-flag bugs). During decoding, `flags` and `version`
/// are populated from the wire bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedHeader {
    pub opcode: Opcode,
    /// Protocol version. Set from the wire on decode; defaults to PROTOCOL_VERSION on encode.
    pub version: u8,
    /// Flags byte. Computed from Option fields on encode; set from wire on decode.
    pub flags: u8,
}
