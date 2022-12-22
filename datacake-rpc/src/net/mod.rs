mod client;
mod server;
mod status;
mod tls;
mod utils;

use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;

pub use client::{ClientConnectError, ClientConnection};
use quinn::{ReadError, RecvStream, SendStream};
use rkyv::AlignedVec;
pub(crate) use server::start_rpc_server;
pub use server::ServerBindError;
pub use status::{ArchivedErrorCode, ArchivedStatus, ErrorCode, Status};

use crate::net::utils::{MessageKind, HEADER_SIZE};
use crate::request::MessageMetadata;

pub const BUFFER_SIZE: usize = 32 << 10;

pub enum SendMsgError {
    IoError(io::Error),
    Status(Status),
}

/// A multiplexed client connection.
pub(crate) struct ConnectionChannel {
    remote_addr: SocketAddr,
    send: SendStream,
    recv: RecvStream,
    buf: Vec<u8>,
}

impl ConnectionChannel {
    #[inline]
    /// The address of the remote connection.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Sends a message payload across the channel to the server.
    pub(crate) async fn send_msg(
        &mut self,
        metadata: &MessageMetadata,
        msg: &[u8],
    ) -> Result<(), SendMsgError> {
        let buffer =
            utils::serialize_message(metadata, msg).map_err(SendMsgError::Status)?;

        self.send
            .write_all(&buffer)
            .await
            .map_err(|e| SendMsgError::IoError(e.into()))?;
        Ok(())
    }

    /// Sends a message payload across the channel to the server.
    pub(crate) async fn send_error(&mut self, status: &Status) -> io::Result<()> {
        self.send.write_all(&utils::serialize_error(status)).await?;
        Ok(())
    }

    /// Receives a message payload from the server.
    pub(crate) async fn recv_msg(
        &mut self,
    ) -> io::Result<Option<Result<(MessageMetadata, AlignedVec), AlignedVec>>> {
        let mut buffer = Box::new([0u8; BUFFER_SIZE]);
        let mut skip = 0;
        let mut end_pos = 0;

        // Seek upto the first message.
        let res = loop {
            if self
                .extend_buffer_pos(&mut buffer[..], HEADER_SIZE + skip)
                .await?
                == 0
            {
                break Ok(None);
            }

            // Skip invalid bytes.
            match utils::parse_header(&self.buf[skip..]) {
                None => {
                    // Advance the skip cursor.
                    skip += HEADER_SIZE;
                    continue;
                },
                Some(MessageKind::Payload {
                    meta: metadata_len,
                    data: msg_len,
                }) => {
                    let skip_n_bytes = HEADER_SIZE + skip;
                    end_pos = skip_n_bytes + msg_len + metadata_len;
                    if self.extend_buffer_pos(&mut buffer[..], end_pos).await? == 0 {
                        break Ok(None);
                    }

                    let metadata: MessageMetadata = {
                        rkyv::from_bytes(&self.buf[skip_n_bytes..end_pos]).map_err(
                            |_| {
                                io::Error::new(
                                    ErrorKind::InvalidData,
                                    "Message missing metadata payload.",
                                )
                            },
                        )?
                    };

                    let mut buffer = AlignedVec::with_capacity(msg_len);
                    buffer.extend_from_slice(
                        &self.buf[skip_n_bytes + metadata_len..end_pos],
                    );

                    break Ok(Some(Ok((metadata, buffer))));
                },
                Some(MessageKind::Error { data: error_len }) => {
                    let skip_n_bytes = HEADER_SIZE + skip;
                    end_pos = skip_n_bytes + error_len;

                    if error_len == 0 {
                        self.shift_buffer_to(end_pos);
                        break Ok(Some(Err(AlignedVec::new())));
                    }

                    if self.extend_buffer_pos(&mut buffer[..], end_pos).await? == 0 {
                        break Ok(None);
                    }

                    let mut buffer = AlignedVec::with_capacity(error_len);
                    buffer.extend_from_slice(&self.buf[skip_n_bytes..end_pos]);

                    break Ok(Some(Err(buffer)));
                },
            }
        };

        self.shift_buffer_to(end_pos);

        res
    }

    fn shift_buffer_to(&mut self, n: usize) {
        let remaining_len = self.buf.len() - n;
        self.buf.copy_within(n.., 0);
        self.buf.truncate(remaining_len);
        self.buf.shrink_to_fit();
    }

    async fn extend_buffer_pos(
        &mut self,
        buffer: &mut [u8],
        min_len: usize,
    ) -> Result<usize, ReadError> {
        let mut total_read = 0;
        while self.buf.len() <= min_len {
            match self.recv.read(&mut buffer[..]).await? {
                None => return Ok(0),
                Some(n) => {
                    self.buf.extend_from_slice(&buffer[..n]);
                    total_read += n;
                },
            };
        }

        Ok(total_read)
    }
}
