use crate::buffer_slice::{BufferSlice};
use crate::{hpack, http2};
use crate::http2::HTTP2Client;

/*
pub struct GRPCCompressor {
    compressor: CompressorOxide
}

impl GRPCCompressor {
    pub fn new() -> Self {
        Self {
            compressor: CompressorOxide::new(create_comp_flags_from_zip_params(CompressionLevel::BestSpeed as i32, 1, CompressionStrategy::Default as i32))
        }
    }

    pub fn zlib_deflate<'b>(&mut self, slices: &[&[u8]], buffer: &'b mut [u8]) -> &'b [u8] {
        self.compressor.reset();
        let mut bytes_written = 0;
        for i in 0..slices.len() {
            let flush_mode = if i < slices.len()-1 {TDEFLFlush::Sync} else {TDEFLFlush::Finish};
            let (status, input_pos, output_pos) = compress(
                self.compressor,
                slices[i],
                &mut buffer[bytes_written..],
                flush_mode);
            bytes_written += output_pos;
            assert_eq!(input_pos, slices[i].len());
            assert_eq!(status, match flush_mode {TDEFLFlush::Finish => TDEFLStatus::Done, _ => TDEFLStatus::Okay});
        }

        &buffer[..bytes_written]
    }
}

 */

pub struct GRPCCompressor {

}

impl GRPCCompressor {
    pub fn new() -> Self {
        Self {
        }
    }

    pub fn zlib_deflate<'b>(&mut self, slices: &[&[u8]], buffer: &'b mut [u8]) -> &'b [u8] {
        let mut bytes_written = 0;

        &buffer[0..bytes_written]
    }
}

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

    pub fn compress<'b>(&self, compressor: &mut GRPCCompressor, buffer: &'b mut [u8]) -> GRPCMessage<'b, 1> {
        assert_eq!(self.prefix[0], 0); // Don't compress if already compressed
        GRPCMessage::<1>::from_proto_message(
            BufferSlice::new_from_slice(compressor.zlib_deflate(self.proto_body.to_slice_slice(), buffer)),
            true
        )
    }

    pub fn len(&self) -> usize {
        self.prefix.len() + self.proto_body.len()
    }

    pub fn compressed(&self) -> bool {
        self.prefix[0] == 1
    }
}

pub trait GRPCMessageBoxWritable {
    fn compress_from_slice_slice<'a, 'b>(&'a mut self, compressor: &mut GRPCCompressor, source: &'b [&'b[u8]]);
    fn copy_from_slice_slice<'a, 'b>(&'a mut self, source: &'b [&'b[u8]]);
}



#[derive(Clone, Copy)]
pub struct GRPCMessageBox<const L: usize> {
    buffer: [u8; L],
    length: usize,
    is_compressed: bool
}

impl<const L: usize> GRPCMessageBox<L> {
    pub fn new() -> Self {
        Self {
            buffer: [0; L],
            length: 0,
            is_compressed: false
        }
    }

    /*
    pub fn compress_from<'a, 'b, const L2: usize>(&'a mut self, compressor: &mut GRPCCompressor, source: &'b GRPCMessage<'b, L2>) {
        assert!(!source.compressed());
        let new_slice = source.proto_body.zlib_deflate(&mut compressor.compressor, &mut self.buffer);
        self.length = new_slice.len();
        self.is_compressed = true;
    }*/

    pub fn get_message(&self) -> GRPCMessage<'_, 1> {
        GRPCMessage::from_proto_message(BufferSlice::new_from_slice(&self.buffer[..self.length]), self.is_compressed)
    }
}

impl<const L: usize> GRPCMessageBoxWritable for GRPCMessageBox<L> {
    fn compress_from_slice_slice<'a, 'b>(&'a mut self, compressor: &mut GRPCCompressor, source: &'b [&'b[u8]]) {
        let new_slice = compressor.zlib_deflate(source, &mut self.buffer);
        self.length = new_slice.len();
        self.is_compressed = true;
    }

    fn copy_from_slice_slice<'a, 'b>(&'a mut self, source: &'b [&'b[u8]]) {
        self.length = 0;
        for x in source {
            self.buffer[self.length..self.length+x.len()].copy_from_slice(x);
            self.length += x.len();
        }
        self.is_compressed = false;
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
        message: &GRPCMessage<'b, L>,
        end_call: bool) -> Result<(), http2::Error>
        where
            [(); {L+1}]:,
            [(); {L+1+1}]:
    {
        self.http2_client.send_data(&grpc_call.http2_stream, message.get_wire_message(), end_call).await
    }

    pub async fn new_call(&mut self, grpc_path: &str) -> Result<GRPCCall, http2::Error> {
        let mut header_list = hpack::PlaintextHeaderList::<7>::new();

        header_list.add_header(":method",  "POST");
        header_list.add_header(":scheme",  "http");
        header_list.add_header(":path", grpc_path);
        header_list.add_header(":authority", "no");
        header_list.add_header("te",  "trailers");
        header_list.add_header("content-type",  "application/grpc+proto");
        header_list.add_header("grpc-encoding",  "identity");


        let http2_stream = self.http2_client.new_outbound_stream(header_list).await?;
        Ok(GRPCCall {
            http2_stream
        })
    }

    pub async fn lossy_receive_loop(&mut self) {
        self.http2_client.lossy_receive_loop().await;
    }

    pub async fn close_call(&mut self, grpc_call: GRPCCall) {
        self.http2_client.close_stream(grpc_call.http2_stream).await;
    }
}

pub struct GRPCCall {
    http2_stream: http2::HTTP2Stream
}