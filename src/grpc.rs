use crate::buffer_slice::BufferSlice;
use crate::{hpack, http2};
use crate::http2::HTTP2Client;

pub struct GRPCMessage<'a, const L: usize> {
    prefix: [u8; 5],
    proto_body: BufferSlice<'a, L>
}

impl<'a, const L: usize> GRPCMessage<'a, L> {
    pub fn from_proto_message(proto_body: BufferSlice<'a, L>, is_compressed: bool) -> Self {
        let mut prefix = [0u8; 5];
        prefix[0] = is_compressed as u8;
        let proto_message_length = proto_body.len();
        prefix[1] = (proto_message_length >> 24) as u8;
        prefix[2] = (proto_message_length >> 16) as u8;
        prefix[3] = (proto_message_length >> 8) as u8;
        prefix[4] = (proto_message_length >> 0) as u8;
        Self {
            prefix,
            proto_body
        }
    }

    pub fn get_wire_message(&'a self) -> BufferSlice<'a, {L+1}> {
        self.proto_body.prepend_slice(&self.prefix)
    }

    pub fn get_proto_message(&self) -> (BufferSlice<'a, L>, bool) {
        let is_compressed = self.prefix[0] == 1;
        (self.proto_body.clone(), is_compressed)
    }

    pub fn from_wire_message(wire_message: &mut BufferSlice<'a, L>) -> Self {
        let mut prefix: [u8; 5] = Default::default();
        for i in 0..5 {
            prefix[i] = wire_message[i];
        }
        let message_len = (prefix[1] as usize) << 24 | (prefix[2] as usize) << 16 | (prefix[3] as usize) << 8 | (prefix[4] as usize) << 0;
        let (proto_body, rest) = wire_message.split_at(5).1.split_at(message_len);
        *wire_message = rest;
        Self {
            prefix,
            proto_body
        }
    }
}

pub struct GRPCClient<'a, T>
where
    T: embedded_io_async::Write + embedded_io_async::Read
{
    pub http2_client: HTTP2Client<'a, T>
}

impl<'a, T> GRPCClient<'a, T>
where
    T: embedded_io_async::Write + embedded_io_async::Read
{

    pub async fn new(connection: &'a mut T) -> Result<Self, http2::Error> {
        let http2_client = HTTP2Client::new(connection).await?;
        Ok(GRPCClient {
            http2_client
        })
    }

    pub async fn send_message<'b, const L: usize> (
        &mut self,
        grpc_call: &GRPCCall,
        message: GRPCMessage<'b, L>,
        end_call: bool) -> Result<(), http2::Error>
        where
            [(); {L+1}]:,
            [(); {L+1+1}]:
    {
        self.http2_client.send_data(&grpc_call.http2_stream, message.get_wire_message(), end_call).await
    }

    pub async fn new_call(&mut self, grpc_path: &str) -> Result<GRPCCall, http2::Error> {
        let mut header_list = hpack::PlaintextHeaderList::<6>::new();

        header_list.add_header(":method",  "POST");
        header_list.add_header(":scheme",  "http");
        header_list.add_header(":path", grpc_path);
        header_list.add_header(":authority", "no");
        header_list.add_header("te",  "trailers");
        header_list.add_header("content-type",  "application/grpc+proto");

        let http2_stream = self.http2_client.new_outbound_stream(header_list).await?;
        Ok(GRPCCall {
            http2_stream
        })
    }
}

pub struct GRPCCall {
    http2_stream: http2::HTTP2Stream
}