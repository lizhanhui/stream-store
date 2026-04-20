use bytes::{BufMut, BytesMut};
use common::types::{HEADER_LEN, MAGIC, PROTOCOL_VERSION};

use super::{Frame, VariableHeader};

impl Frame {
    /// Compute the remaining length (variable header + payload) for this frame.
    pub(super) fn remaining_length(&self) -> u32 {
        let vh = self.variable_header_len();
        let pl = if self.has_payload_section() {
            4 + self.payload.as_ref().map_or(0, |p| p.len()) // u32 length prefix + payload bytes
        } else {
            0
        };
        (vh + pl) as u32
    }

    /// Variable header size in bytes for this frame's opcode+flags.
    fn variable_header_len(&self) -> usize {
        match &self.variable_header {
            // request_id(4) + stream_id(4) + epoch(4)
            VariableHeader::Append { .. } => 4 + 4 + 4,
            // request_id(4) + stream_id(4) + epoch(4) + extent_id(4) + offset(8)
            VariableHeader::AppendAck { .. } => 4 + 4 + 4 + 4 + 8,
            // request_id(4) + stream_id(4) + epoch(4) + extent_id(4) + error_code(2)
            VariableHeader::AppendAckError { .. } => 4 + 4 + 4 + 4 + 2,
            // request_id(4) + stream_id(4) + extent_id(4) + offset(8) + count(4)
            VariableHeader::Read { .. } => 4 + 4 + 4 + 8 + 4,
            // request_id(4) + stream_id(4) + offset(8) + count(4)
            VariableHeader::ReadResp { .. } => 4 + 4 + 8 + 4,
            // request_id(4) + stream_id(4) + extent_id(4) + offset(8) + error_code(2)
            VariableHeader::ReadRespError { .. } => 4 + 4 + 4 + 8 + 2,
            // request_id(4) + stream_id(4) + epoch(4)
            VariableHeader::SealStreamManagerRequest { .. } => 4 + 4 + 4,
            // request_id(4) + stream_id(4) + offset(8) + new_epoch(4) + addr_len(2) + addr(N)
            VariableHeader::SealStreamManagerResp { primary_addr, .. } => {
                4 + 4 + 8 + 4 + 2 + primary_addr.len()
            }
            // request_id(4) + stream_id(4) + error_code(2)
            VariableHeader::SealStreamManagerRespError { .. } => 4 + 4 + 2,
            // request_id(4) + stream_id(4) + epoch(4) + extent_id_from(4) + start_offset(8)
            VariableHeader::SealExtentNodePrepare { .. } => 4 + 4 + 4 + 4 + 8,
            // request_id(4) + stream_id(4) + epoch(4) + extent_id(4) + start_offset(8) + end_offset(8)
            VariableHeader::SealExtentNodeResp { .. } => 4 + 4 + 4 + 4 + 8 + 8,
            // request_id(4) + stream_id(4) + error_code(2)
            VariableHeader::SealExtentNodeRespError { .. } => 4 + 4 + 2,
            // stream_id(4) + extent_id(4) + epoch(4) + start_offset(8) + end_offset(8)
            VariableHeader::SealExtentNodeCommit { .. } => 4 + 4 + 4 + 4 + 8 + 8,
            // request_id(4) + stream_id(4) + extent_id(4)
            VariableHeader::SealExtentNodeCommitResp { .. } => 4 + 4 + 4,
            // request_id(4) + name_len(2) + name(N) + replication_factor(1) + min_extent_capacity(4) + max_extent_capacity(4) + cache_extents(2) + extent_growth_factor(1) + storage_class(1)
            VariableHeader::CreateStream { stream_name, .. } => {
                4 + 2 + stream_name.len() + 1 + 4 + 4 + 2 + 1 + 1
            }
            // request_id(4) + stream_id(4) + extent_id(4) + epoch(4) + addr_len(2) + addr(N)
            VariableHeader::CreateStreamResp { primary_addr, .. } => {
                4 + 4 + 4 + 4 + 2 + primary_addr.len()
            }
            // request_id(4) + error_code(2)
            VariableHeader::CreateStreamRespError { .. } => 4 + 2,
            // request_id(4) + stream_id(4)
            VariableHeader::QueryOffset { .. } => 4 + 4,
            // request_id(4) + stream_id(4) + offset(8)
            VariableHeader::QueryOffsetResp { .. } => 4 + 4 + 8,
            // request_id(4) + stream_id(4) + error_code(2)
            VariableHeader::QueryOffsetRespError { .. } => 4 + 4 + 2,
            // request_id(4)
            VariableHeader::Connect { .. }
            | VariableHeader::ConnectAck { .. }
            | VariableHeader::Disconnect { .. }
            | VariableHeader::DisconnectAck { .. }
            | VariableHeader::Heartbeat { .. } => 4,
            // request_id(4) + error_code(2)
            VariableHeader::ConnectAckError { .. }
            | VariableHeader::DisconnectAckError { .. }
            | VariableHeader::HeartbeatError { .. } => 4 + 2,
            // request_id(4) + stream_id(4) + extent_id(4) + role(1) + replication_factor(1) + epoch(4) + cache_extents(2) + min_extent_capacity(4) + max_extent_capacity(4) + extent_growth_factor(1) + storage_class(1)
            VariableHeader::RegisterExtent { .. } => 4 + 4 + 4 + 1 + 1 + 4 + 2 + 4 + 4 + 1 + 1,
            // request_id(4) + stream_id(4) + extent_id(4)
            VariableHeader::RegisterExtentAck { .. } => 4 + 4 + 4,
            // request_id(4) + stream_id(4) + extent_id(4) + error_code(2)
            VariableHeader::RegisterExtentAckError { .. } => 4 + 4 + 4 + 2,
            // stream_id(4) + extent_id(4) + epoch(4) + offset(8)
            VariableHeader::Watermark { .. } => 4 + 4 + 4 + 8,
            // stream_id(4) + epoch(4) + sealed_extent_id(4) + end_offset(8) + new_extent_id(4) + new_extent_capacity(4)
            VariableHeader::UpdateExtentSealed { .. } => 4 + 4 + 4 + 8 + 4 + 4,
            // stream_id(4) + epoch(4) + extent_id(4) + current_offset(8)
            VariableHeader::UpdateExtentProgress { .. } => 4 + 4 + 4 + 8,
            // stream_id(4) + epoch(4) + extent_id(4)
            VariableHeader::UpdateExtentFlushed { .. } => 4 + 4 + 4,
            // request_id(4) + stream_id(4) + epoch(4)
            VariableHeader::ReportExtents { .. } => 4 + 4 + 4,
            // request_id(4) + stream_id(4) + epoch(4)
            VariableHeader::ReportExtentsResp { .. } => 4 + 4 + 4,
            // request_id(4) + stream_id(4) + epoch(4) + error_code(2)
            VariableHeader::ReportExtentsRespError { .. } => 4 + 4 + 4 + 2,
            // stream_id(4) + extent_id(4) + epoch(4) + offset(8) + byte_pos(8)
            VariableHeader::Forward { .. } => 4 + 4 + 4 + 8 + 8,
            // stream_id(4) + extent_id(4) + epoch(4) + start_offset(8) + extent_capacity(4) + cache_extents(2) + min_extent_capacity(4) + max_extent_capacity(4) + extent_growth_factor(1) + storage_class(1)
            VariableHeader::ForwardInitExtent { .. } => 4 + 4 + 4 + 8 + 4 + 2 + 4 + 4 + 1 + 1,
            // stream_id(4) + extent_id(4) + epoch(4) + checksum(4) + committed_bytes(8)
            VariableHeader::ForwardChecksum { .. } => 4 + 4 + 4 + 4 + 8,
            // stream_id(4) + extent_id(4) + epoch(4)
            VariableHeader::ForwardFlushed { .. } => 4 + 4 + 4,
            // request_id(4) + stream_id(4) + extent_id(4) + epoch(4) + start_offset(8) + end_offset(8)
            VariableHeader::FlushExtent { .. } => 4 + 4 + 4 + 4 + 8 + 8,
            // request_id(4) + stream_id(4) + extent_id(4)
            VariableHeader::FlushExtentResp { .. } => 4 + 4 + 4,
            // request_id(4) + stream_id(4) + extent_id(4) + error_code(2)
            VariableHeader::FlushExtentRespError { .. } => 4 + 4 + 4 + 2,
            // no variable header, just payload
            VariableHeader::StreamManagerMembershipChange => 0,
            // request_id(4) + stream_id(4) + count(4) [+ name_len(2) + name(N) if FLAG_DESCRIBE_STREAM_BY_NAME]
            VariableHeader::DescribeStream { stream_name, .. } => {
                let base = 4 + 4 + 4;
                let name = stream_name.as_ref().map_or(0, |n| 2 + n.len());
                base + name
            }
            // request_id(4) + stream_id(4)
            VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeExtentResp { .. } => 4 + 4,
            // request_id(4) + stream_id(4) + error_code(2)
            VariableHeader::DescribeStreamRespError { .. } => 4 + 4 + 2,
            // request_id(4) + stream_id(4) + extent_id(4) + error_code(2)
            VariableHeader::DescribeExtentRespError { .. } => 4 + 4 + 4 + 2,
            // request_id(4) + stream_id(4) + extent_id(4)
            VariableHeader::DescribeExtent { .. } => 4 + 4 + 4,
            // request_id(4) + stream_id(4) + offset(8)
            VariableHeader::Seek { .. } | VariableHeader::SeekResp { .. } => 4 + 4 + 8,
            // request_id(4) + stream_id(4) + offset(8) + error_code(2)
            VariableHeader::SeekRespError { .. } => 4 + 4 + 8 + 2,
        }
    }

    /// Whether this opcode has a payload section (u32 length prefix + bytes).
    pub(super) fn has_payload_section(&self) -> bool {
        match &self.variable_header {
            VariableHeader::Append { .. }
            | VariableHeader::ReadResp { .. }
            | VariableHeader::ReadRespError { .. }
            | VariableHeader::Connect { .. }
            | VariableHeader::Disconnect { .. }
            | VariableHeader::Heartbeat { .. }
            | VariableHeader::HeartbeatError { .. }
            | VariableHeader::RegisterExtent { .. }
            | VariableHeader::Forward { .. }
            | VariableHeader::StreamManagerMembershipChange
            | VariableHeader::ReportExtentsResp { .. }
            | VariableHeader::ReportExtentsRespError { .. }
            | VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeStreamRespError { .. }
            | VariableHeader::DescribeExtentResp { .. }
            | VariableHeader::DescribeExtentRespError { .. }
            | VariableHeader::SeekResp { .. }
            | VariableHeader::SeekRespError { .. }
            | VariableHeader::AppendAckError { .. }
            | VariableHeader::SealStreamManagerRespError { .. }
            | VariableHeader::SealExtentNodeResp { .. }
            | VariableHeader::SealExtentNodeRespError { .. }
            | VariableHeader::CreateStreamRespError { .. }
            | VariableHeader::QueryOffsetRespError { .. }
            | VariableHeader::ConnectAckError { .. }
            | VariableHeader::DisconnectAckError { .. }
            | VariableHeader::RegisterExtentAckError { .. }
            | VariableHeader::FlushExtentRespError { .. } => true,
            _ => false,
        }
    }

    /// Encode this frame into the destination buffer.
    pub fn encode(&self, dst: &mut BytesMut) {
        let remaining = self.remaining_length();
        dst.reserve(HEADER_LEN + remaining as usize);

        // Fixed header (8 bytes).
        dst.put_u8(MAGIC);
        dst.put_u8(PROTOCOL_VERSION);
        dst.put_u8(self.header.opcode as u8);
        dst.put_u8(self.flags());
        dst.put_u32(remaining);

        // Variable header (opcode-specific).
        self.encode_variable_header(dst);

        // Payload section (u32 length prefix + bytes), if applicable.
        if self.has_payload_section() {
            let payload_bytes = self.payload.as_ref().map_or(&[][..], |p| &p[..]);
            dst.put_u32(payload_bytes.len() as u32);
            dst.extend_from_slice(payload_bytes);
        }
    }

    fn encode_variable_header(&self, dst: &mut BytesMut) {
        match &self.variable_header {
            VariableHeader::Append {
                request_id,
                stream_id,
                epoch,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
            }
            VariableHeader::AppendAck {
                request_id,
                stream_id,
                epoch,
                extent_id,
                offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::AppendAckError {
                request_id,
                stream_id,
                epoch,
                extent_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(extent_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::Read {
                request_id,
                stream_id,
                extent_id,
                offset,
                count,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(offset.0);
                dst.put_u32(*count);
            }
            VariableHeader::ReadResp {
                request_id,
                stream_id,
                offset,
                count,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u64(offset.0);
                dst.put_u32(*count);
            }
            VariableHeader::ReadRespError {
                request_id,
                stream_id,
                extent_id,
                offset,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(offset.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::SealStreamManagerRequest {
                request_id,
                stream_id,
                epoch,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
            }
            VariableHeader::SealStreamManagerResp {
                request_id,
                stream_id,
                offset,
                new_epoch,
                primary_addr,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u64(offset.0);
                dst.put_u32(new_epoch.0);
                dst.put_u16(primary_addr.len() as u16);
                dst.extend_from_slice(primary_addr);
            }
            VariableHeader::SealStreamManagerRespError {
                request_id,
                stream_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::SealExtentNodePrepare {
                request_id,
                stream_id,
                epoch,
                extent_id_from,
                start_offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(extent_id_from.0);
                dst.put_u64(*start_offset);
            }
            VariableHeader::SealExtentNodeResp {
                request_id,
                stream_id,
                epoch,
                extent_id,
                start_offset,
                end_offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(*start_offset);
                dst.put_u64(*end_offset);
            }
            VariableHeader::SealExtentNodeRespError {
                request_id,
                stream_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::SealExtentNodeCommit {
                request_id,
                stream_id,
                extent_id,
                epoch,
                start_offset,
                end_offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
                dst.put_u64(*start_offset);
                dst.put_u64(*end_offset);
            }
            VariableHeader::SealExtentNodeCommitResp {
                request_id,
                stream_id,
                extent_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
            }
            VariableHeader::CreateStream {
                request_id,
                stream_name,
                replication_factor,
                min_extent_capacity,
                max_extent_capacity,
                cache_extents,
                extent_growth_factor,
                storage_class,
            } => {
                dst.put_u32(*request_id);
                dst.put_u16(stream_name.len() as u16);
                dst.extend_from_slice(stream_name);
                dst.put_u8(*replication_factor);
                dst.put_u32(*min_extent_capacity);
                dst.put_u32(*max_extent_capacity);
                dst.put_u16(*cache_extents);
                dst.put_u8(*extent_growth_factor);
                dst.put_u8(storage_class.as_u8());
            }
            VariableHeader::CreateStreamResp {
                request_id,
                stream_id,
                extent_id,
                epoch,
                primary_addr,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
                dst.put_u16(primary_addr.len() as u16);
                dst.extend_from_slice(primary_addr);
            }
            VariableHeader::CreateStreamRespError {
                request_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::QueryOffset {
                request_id,
                stream_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
            }
            VariableHeader::QueryOffsetResp {
                request_id,
                stream_id,
                offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::QueryOffsetRespError {
                request_id,
                stream_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::Connect { request_id }
            | VariableHeader::Disconnect { request_id }
            | VariableHeader::Heartbeat { request_id } => {
                dst.put_u32(*request_id);
            }
            VariableHeader::RegisterExtent {
                request_id,
                stream_id,
                extent_id,
                role,
                replication_factor,
                epoch,
                cache_extents,
                min_extent_capacity,
                max_extent_capacity,
                extent_growth_factor,
                storage_class,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u8(*role);
                dst.put_u8(*replication_factor);
                dst.put_u32(epoch.0);
                dst.put_u16(*cache_extents);
                dst.put_u32(*min_extent_capacity);
                dst.put_u32(*max_extent_capacity);
                dst.put_u8(*extent_growth_factor);
                dst.put_u8(storage_class.as_u8());
            }
            VariableHeader::ConnectAck { request_id }
            | VariableHeader::DisconnectAck { request_id } => {
                dst.put_u32(*request_id);
            }
            VariableHeader::ConnectAckError {
                request_id,
                error_code,
            }
            | VariableHeader::DisconnectAckError {
                request_id,
                error_code,
            }
            | VariableHeader::HeartbeatError {
                request_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::RegisterExtentAck {
                request_id,
                stream_id,
                extent_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
            }
            VariableHeader::RegisterExtentAckError {
                request_id,
                stream_id,
                extent_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::Watermark {
                stream_id,
                extent_id,
                epoch,
                offset,
            } => {
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::Forward {
                stream_id,
                extent_id,
                epoch,
                offset,
                byte_pos,
            } => {
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
                dst.put_u64(offset.0);
                dst.put_u64(*byte_pos);
            }
            VariableHeader::ForwardInitExtent {
                stream_id,
                extent_id,
                epoch,
                start_offset,
                extent_capacity,
                cache_extents,
                min_extent_capacity,
                max_extent_capacity,
                extent_growth_factor,
                storage_class,
            } => {
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
                dst.put_u64(start_offset.0);
                dst.put_u32(*extent_capacity);
                dst.put_u16(*cache_extents);
                dst.put_u32(*min_extent_capacity);
                dst.put_u32(*max_extent_capacity);
                dst.put_u8(*extent_growth_factor);
                dst.put_u8(storage_class.as_u8());
            }
            VariableHeader::ForwardChecksum {
                stream_id,
                extent_id,
                epoch,
                checksum,
                committed_bytes,
            } => {
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(*checksum);
                dst.put_u64(*committed_bytes);
            }
            VariableHeader::ForwardFlushed {
                stream_id,
                extent_id,
                epoch,
            } => {
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
            }
            VariableHeader::FlushExtent {
                request_id,
                stream_id,
                extent_id,
                epoch,
                start_offset,
                end_offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
                dst.put_u64(*start_offset);
                dst.put_u64(*end_offset);
            }
            VariableHeader::FlushExtentResp {
                request_id,
                stream_id,
                extent_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
            }
            VariableHeader::FlushExtentRespError {
                request_id,
                stream_id,
                extent_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::StreamManagerMembershipChange => {
                // no variable header fields, just payload
            }
            VariableHeader::DescribeStream {
                request_id,
                stream_id,
                count,
                stream_name,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(*count);
                if let Some(name) = stream_name {
                    dst.put_u16(name.len() as u16);
                    dst.extend_from_slice(name);
                }
            }
            VariableHeader::DescribeStreamResp {
                request_id,
                stream_id,
            }
            | VariableHeader::DescribeExtentResp {
                request_id,
                stream_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
            }
            VariableHeader::DescribeStreamRespError {
                request_id,
                stream_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::DescribeExtentRespError {
                request_id,
                stream_id,
                extent_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::DescribeExtent {
                request_id,
                stream_id,
                extent_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(extent_id.0);
            }
            VariableHeader::Seek {
                request_id,
                stream_id,
                offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::SeekResp {
                request_id,
                stream_id,
                offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::SeekRespError {
                request_id,
                stream_id,
                offset,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u64(offset.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::UpdateExtentSealed {
                stream_id,
                epoch,
                sealed_extent_id,
                end_offset,
                new_extent_id,
                new_extent_capacity,
            } => {
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(sealed_extent_id.0);
                dst.put_u64(end_offset.0);
                dst.put_u32(new_extent_id.0);
                dst.put_u32(*new_extent_capacity);
            }
            VariableHeader::UpdateExtentProgress {
                stream_id,
                epoch,
                extent_id,
                current_offset,
            } => {
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(current_offset.0);
            }
            VariableHeader::UpdateExtentFlushed {
                stream_id,
                epoch,
                extent_id,
            } => {
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(extent_id.0);
            }
            VariableHeader::ReportExtents {
                request_id,
                stream_id,
                epoch,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
            }
            VariableHeader::ReportExtentsResp {
                request_id,
                stream_id,
                epoch,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
            }
            VariableHeader::ReportExtentsRespError {
                request_id,
                stream_id,
                epoch,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u32(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u16(*error_code as u16);
            }
        }
    }
}
