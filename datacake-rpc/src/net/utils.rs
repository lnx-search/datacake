use std::mem;

use crate::request::MessageMetadata;
use crate::Status;

static HEADER_TERMINATOR: &[u8] = b"\r\n";

pub const KIND_SIZE: usize = 1;
pub const META_LEN_SIZE: usize = mem::size_of::<u16>();
pub const DATA_LEN_SIZE: usize = mem::size_of::<u32>();
pub const LEN_SIZE: usize = KIND_SIZE + META_LEN_SIZE + DATA_LEN_SIZE;
pub const HEADER_SIZE: usize = LEN_SIZE + 2;
pub const MESSAGE_KIND_PAYLOAD: u8 = 0;
pub const MESSAGE_KIND_ERROR: u8 = 1;

pub(crate) enum MessageKind {
    Payload { meta: usize, data: usize },
    Error { data: usize },
}

/// Parses a message header from the channel.
///
/// Returns `None` if the head of the buffer does not match
/// the header layout.
pub(crate) fn parse_header(buf: &[u8]) -> Option<MessageKind> {
    let header = &buf[..HEADER_SIZE];

    if &header[LEN_SIZE..] != HEADER_TERMINATOR {
        return None;
    }

    let data_size_bytes = &header[KIND_SIZE + META_LEN_SIZE..KIND_SIZE + META_LEN_SIZE + DATA_LEN_SIZE];
    let data_size = u32::from_le_bytes(data_size_bytes.try_into().ok()?);
    let kind = match header[0] {
        0 => {
            let meta_size_bytes = &header[KIND_SIZE..KIND_SIZE + META_LEN_SIZE];
            let meta_size = u16::from_le_bytes(meta_size_bytes.try_into().ok()?);
            MessageKind::Payload {
                meta: meta_size as usize,
                data: data_size as usize,
            }
        },
        1 => MessageKind::Error {
            data: data_size as usize,
        },
        _ => return None,
    };

    Some(kind)
}

fn serialize_header(
    kind: u8,
    metadata_len: usize,
    data_size: usize,
) -> [u8; HEADER_SIZE] {
    let mut header = [0; HEADER_SIZE];
    header[0] = kind;
    header[KIND_SIZE..KIND_SIZE + META_LEN_SIZE]
        .copy_from_slice(&(metadata_len as u16).to_le_bytes());
    header[KIND_SIZE + META_LEN_SIZE..KIND_SIZE + META_LEN_SIZE + DATA_LEN_SIZE]
        .copy_from_slice(&(data_size as u32).to_le_bytes());
    header[KIND_SIZE + META_LEN_SIZE + DATA_LEN_SIZE..]
        .copy_from_slice(HEADER_TERMINATOR);
    header
}

/// Serializes a message payload into a buffer.
pub(crate) fn serialize_message(
    metadata: &MessageMetadata,
    msg_bytes: &[u8],
) -> Result<Vec<u8>, Status> {
    let metadata_bytes =
        rkyv::to_bytes::<_, 2048>(metadata).map_err(|_| Status::invalid())?;

    let header =
        serialize_header(MESSAGE_KIND_PAYLOAD, metadata_bytes.len(), msg_bytes.len());

    let mut buffer =
        Vec::with_capacity(HEADER_SIZE + metadata_bytes.len() + msg_bytes.len());
    buffer.extend_from_slice(&header);
    buffer.extend_from_slice(&metadata_bytes);
    buffer.extend_from_slice(msg_bytes);

    Ok(buffer)
}

/// Serializes a error payload into a buffer, if the error serialization fails
/// a default error is produced.
pub(crate) fn serialize_error(status: &Status) -> Vec<u8> {
    match serialize_error_inner(status) {
        Ok(buffer) => buffer,
        Err(error) => {
            warn!(error = ?error, "Failed to serialize error payload, returning default...");
            serialize_header(MESSAGE_KIND_ERROR, 0, 0).to_vec()
        },
    }
}

/// Attempts to serializes a error payload into a buffer.
fn serialize_error_inner(status: &Status) -> Result<Vec<u8>, Status> {
    let status_bytes =
        rkyv::to_bytes::<_, 2048>(status).map_err(|_| Status::invalid())?;

    let header = serialize_header(MESSAGE_KIND_ERROR, 0, status_bytes.len());

    let mut buffer = Vec::with_capacity(HEADER_SIZE + status_bytes.len());
    buffer.extend_from_slice(&header);
    buffer.extend_from_slice(&status_bytes);

    Ok(buffer)
}
