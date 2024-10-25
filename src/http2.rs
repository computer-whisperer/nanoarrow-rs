use core::fmt::Formatter;
use embedded_nal_async::TcpConnect;
use embedded_io_async::{Write, Read, ReadExactError};
use crate::hpack;
use crate::buffer_slice::{BufferSlice};
use crate::hpack::PlaintextHeaderList;

pub const HTTP2_CONNECTION_PREFACE: &str = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";


#[derive(Debug)]
pub enum Error {
    ReadError,
    WriteError
}

#[derive(Debug)]
pub struct DataFrame<'a, const L: usize> {
    stream_id: u32,
    padding_length: Option<u8>,
    end_stream: bool,
    data: BufferSlice<'a, L>
}

impl<'a, const L: usize> DataFrame<'a, L> {
    pub fn new(stream_id: u32, end_stream: bool, data: BufferSlice<'a, L>) -> Self {
        DataFrame {
            stream_id,
            padding_length: None,
            end_stream,
            data,
        }
    }
}

pub struct HeaderFrame<'a> {
    stream_id: u32,
    padding_length: Option<u8>,
    end_stream: bool,
    end_headers: bool,
    priority_weight: Option<u8>,
    stream_dependency: Option<u32>,
    exclusive: bool,
    block_fragment: &'a [u8],
}

impl<'a> HeaderFrame<'a> {
    pub fn new(stream_id: u32, end_stream: bool, end_headers: bool, block_fragment: &'a [u8]) -> Self {
        HeaderFrame {
            stream_id,
            padding_length: None,
            end_stream,
            end_headers,
            priority_weight: None,
            stream_dependency: None,
            exclusive: false,
            block_fragment
        }
    }

    pub fn get_block_fragment(&self) -> &'a [u8] {
        self.block_fragment
    }
}

impl core::fmt::Debug for HeaderFrame<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        let encoded = self.get_block_fragment();
        let mut decode_buffer = [0u8; 200];
        let mut decoder = hpack::Decoder::new();
        let header_list = decoder.decode_header_list::<20>(encoded, &mut decode_buffer);
        if self.end_headers {
            f.debug_struct("HeaderFrame")
                .field("steam_id", &self.stream_id)
                .field("padding_length", &self.padding_length)
                .field("end_stream", &self.end_stream)
                .field("end_headers", &self.end_headers)
                .field("priority_weight", &self.priority_weight)
                .field("stream_dependency", &self.stream_dependency)
                .field("exclusive", &self.exclusive)
                .field("header_list", &header_list)
                .finish()
        }
        else {
            f.debug_struct("HeaderFrame")
                .field("steam_id", &self.stream_id)
                .field("padding_length", &self.padding_length)
                .field("end_stream", &self.end_stream)
                .field("end_headers", &self.end_headers)
                .field("priority_weight", &self.priority_weight)
                .field("stream_dependency", &self.stream_dependency)
                .field("exclusive", &self.exclusive)
                .field("block_fragment", &self.block_fragment)
                .finish()
        }

    }
}

#[derive(Debug)]
pub struct RstStreamFrame {
    stream_id: u32,
    error_code: u32
}

#[derive(Debug)]
pub struct SettingsFrame {
    ack: bool
}

impl SettingsFrame {
    fn new(ack: bool) -> Self {
        SettingsFrame {
            ack
        }
    }

    fn get_ack(&self) -> bool {
        self.ack
    }
}

#[derive(Debug)]
pub struct PingFrame {
    ack: bool,
    opaque_data: [u8; 8]
}

#[derive(Debug)]
pub enum Frame<'a, const L: usize> {
    Data(DataFrame<'a, L>),
    Headers(HeaderFrame<'a>),
    Priority,
    RstStream(RstStreamFrame),
    Settings(SettingsFrame),
    PushPromise,
    Ping(PingFrame),
    GoAway,
    WindowUpdate,
    Continuation
}

impl<'a, const L: usize> Frame<'a, L> {
    fn get_stream_identifier(&self) -> u32 {
        match self {
            Frame::Data(x) => x.stream_id,
            Frame::Headers(x) => x.stream_id,
            Frame::RstStream(x) => x.stream_id,
            Frame::Priority => panic!("Priority frame not supported yet!"),
            Frame::Settings(_) => 0,
            Frame::PushPromise => panic!("Push Promise frame not supported yet!"),
            Frame::Ping(_) => 0,
            Frame::GoAway => panic!("Go Away frame not supported yet!"),
            Frame::WindowUpdate => panic!("Window Update frame not supported yet!"),
            Frame::Continuation => panic!("Continuation frame not supported yet!")
        }
    }

    pub fn encode_frame<'b, 'c>(&'c self, header_buffer: &'b mut [u8; 9], misc_frame_scratch: &'b mut [u8; 4]) -> Result<BufferSlice<'b, {L+1}>, Error>
    where
        'a: 'b,
        'c: 'b
    {
        let payload_len = match self {
            Frame::Data(x) => {
                assert!(x.padding_length.is_none());
                x.data.len()
            }
            Frame::Headers(x) => {
                assert!(x.padding_length.is_none());
                assert!(x.priority_weight.is_none());
                assert!(x.stream_dependency.is_none());
                assert!(!x.exclusive);
                x.block_fragment.len()
            }
            Frame::Priority => panic!("Priority frame not supported yet!"),
            Frame::RstStream(_) => 4,
            Frame::Settings(_) => 0,
            Frame::PushPromise => panic!("Push Promise frame not supported yet!"),
            Frame::Ping(_) => 8,
            Frame::GoAway => panic!("GoAway frame not supported yet!"),
            Frame::WindowUpdate => panic!("Window Update frame not supported yet!"),
            Frame::Continuation => panic!("Continuation frame not supported yet!"),
        };
        assert!(payload_len <= 0x4000); // Max frame size
        header_buffer[0] = ((payload_len >> 16)&0xFF) as u8;
        header_buffer[1] = ((payload_len >> 8)&0xFF) as u8;
        header_buffer[2] = ((payload_len >> 0)&0xFF) as u8;
        header_buffer[3] = match self {
            Frame::Data(_) => {0x00}
            Frame::Headers(_) => {0x01}
            Frame::Priority => {0x02}
            Frame::RstStream(_) => {0x03}
            Frame::Settings(_) => {0x04}
            Frame::PushPromise => {0x05}
            Frame::Ping(_) => {0x06}
            Frame::GoAway => {0x07}
            Frame::WindowUpdate => {0x08}
            Frame::Continuation => {0x09}
        };
        header_buffer[4] = match self {
            Frame::Data(x) => {(x.padding_length.is_some() as u8) << 3 | (x.end_stream as u8)}
            Frame::Headers(x) => {(x.priority_weight.is_some() as u8) << 5 | (x.padding_length.is_some() as u8) << 3 | (x.end_headers as u8) << 2 | (x.end_stream as u8)}
            Frame::Settings(x) => {x.ack as u8}
            Frame::Priority => panic!("Priority frame not supported yet!"),
            Frame::RstStream(_) => 0,
            Frame::PushPromise => panic!("PushPromise frame not supported yet!"),
            Frame::Ping(x) => {x.ack as u8}
            Frame::GoAway => panic!("GoAway frame not supported yet!"),
            Frame::WindowUpdate => panic!("WindowUpdate frame not supported yet!"),
            Frame::Continuation => panic!("Continuation frame not supported yet!")
        };
        let stream_identifier = self.get_stream_identifier();
        assert_eq!(stream_identifier & 0x80000000, 0); // R field must be 0
        header_buffer[5] = ((stream_identifier >> 24)&0xFF) as u8;
        header_buffer[6] = ((stream_identifier >> 16)&0xFF) as u8;
        header_buffer[7] = ((stream_identifier >> 8)&0xFF) as u8;
        header_buffer[8] = ((stream_identifier >> 0)&0xFF) as u8;
        match self {
            Frame::Data(data_frame) => {
                Ok(data_frame.data.prepend_slice(header_buffer))
            }
            Frame::Headers(header_frame) => {
                let mut temp: [&[u8]; {L+1}] = [&[]; {L+1}];
                temp[0] = header_buffer;
                temp[1] = header_frame.block_fragment;
                Ok(BufferSlice::new_from_slice_array(temp))
            }
            Frame::RstStream(rst_stream_frame) => {
                misc_frame_scratch[0] = (rst_stream_frame.error_code>>24) as u8;
                misc_frame_scratch[1] = (rst_stream_frame.error_code>>16) as u8;
                misc_frame_scratch[2] = (rst_stream_frame.error_code>>8) as u8;
                misc_frame_scratch[3] = rst_stream_frame.error_code as u8;
                let mut temp: [&[u8]; {L+1}] = [&[]; {L+1}];
                temp[0] = header_buffer;
                temp[1] = &misc_frame_scratch[0..3];
                Ok(BufferSlice::new_from_slice_array(temp))
            }
            Frame::Priority => {
                panic!("Priority frames are not implemented yet");
            }
            Frame::Settings(_) => {
                let mut temp: [&[u8]; {L+1}] = [&[]; {L+1}];
                temp[0] = header_buffer;
                Ok(BufferSlice::new_from_slice_array(temp))
            } // No data supported yet
            Frame::PushPromise => {
                panic!("PushPromise frames are not implemented yet");
            }
            Frame::Ping(ping_frame) => {
                let mut temp: [&[u8]; {L+1}] = [&[]; {L+1}];
                temp[0] = header_buffer;
                temp[1] = &ping_frame.opaque_data;
                Ok(BufferSlice::new_from_slice_array(temp))
            }
            Frame::GoAway => {
                panic!("GoAway frames are not implemented yet");
            }
            Frame::WindowUpdate => {
                panic!("WindowUpdate frames are not implemented yet");
            }
            Frame::Continuation => {
                panic!("Continuation frames are not implemented yet");
            }
        }
    }
}

impl<'a> Frame<'a, 1> {
    pub fn decode_frame(buffer: &'a [u8]) -> Result<Self, Error> {
        let payload_len: usize = (buffer[0] as usize) << 16 | (buffer[1] as usize) << 8 | (buffer[2] as usize) << 0;
        let frame_type = buffer[3];
        let flags = buffer[4];
        let stream_identifier = (buffer[5] as u32) << 24 | (buffer[6] as u32) << 16 | (buffer[7] as u32) << 8 | (buffer[8] as u32) << 0;
        let payload = &buffer[9..9+payload_len];
        let output = match frame_type {
            0x00 => Frame::Data({
                let has_padding = flags & 0x08 == 0x08;
                if has_padding {
                    let padding_len = payload[0];
                    DataFrame {
                        stream_id: stream_identifier,
                        padding_length: Some(padding_len),
                        end_stream: flags & 0x01 == 0x01,
                        data: BufferSlice::new_from_slice(&payload[1..payload_len-(padding_len as usize)])
                    }
                }
                else {
                    DataFrame {
                        stream_id: stream_identifier,
                        padding_length: None,
                        end_stream: flags & 0x01 == 0x01,
                        data: BufferSlice::new_from_slice(payload)
                    }
                }
            }),
            0x01 => Frame::Headers({ // Headers
                let has_padding = flags & 0x08 == 0x08;
                let (inner_payload, padding_length) = if has_padding {
                    let padding_len = payload[0];
                    (&payload[1..payload_len-padding_len as usize], Some(padding_len))
                }
                else {
                    (payload, None)
                };
                let has_priority = flags & 0x20 == 0x20;
                let (inner_payload, exclusive, stream_dependency, priority_weight) = if has_priority {
                    let exclusive = inner_payload[0] & 0x80 == 0x80;
                    let stream_dependency = (inner_payload[0] as u32)&0x7F << 24 | (inner_payload[1] as u32) << 16 | (inner_payload[2] as u32) << 8 | (inner_payload[3] as u32) << 0;
                    (&inner_payload[5..], exclusive, Some(stream_dependency), Some(inner_payload[4]))
                }
                else {
                    (inner_payload, false, None, None)
                };
                HeaderFrame {
                    stream_id: stream_identifier,
                    padding_length,
                    end_stream: flags & 0x01 == 0x01,
                    end_headers: flags & 0x04 == 0x04,
                    priority_weight,
                    exclusive,
                    stream_dependency,
                    block_fragment: inner_payload
                }
            }),
            0x02 => Frame::Priority,
            0x03 => Frame::RstStream(RstStreamFrame {
                stream_id: stream_identifier,
                error_code: (payload[0] as u32) << 24 | (payload[1] as u32) << 16 | (payload[2] as u32) << 8 | (payload[3] as u32)
            }),
            0x04 => Frame::Settings(SettingsFrame {
                ack: flags & 0x01 == 0x01
            }),
            0x05 => Frame::PushPromise,
            0x06 => Frame::Ping( {
                let mut opaque_data = [0u8; 8];
                opaque_data.copy_from_slice(&payload[0..8]);
                PingFrame {
                    opaque_data,
                    ack: flags & 0x01 == 0x01
                }
            }),
            0x07 => Frame::GoAway,
            0x08 => Frame::WindowUpdate,
            0x09 => Frame::Continuation,
            _ => panic!("Unknown frame type")
        };
        Ok(output)
    }
}


pub struct HTTP2Client<'a, T>
where
    T: embedded_io_async::Write + embedded_io_async::Read
{
    connection: &'a mut T,
    hpack_encoder: hpack::Encoder,
    hpack_decoder: hpack::Decoder,
    largest_stream_id: u32
}

impl<'a, T> HTTP2Client<'a, T> where T: embedded_io_async::Write + embedded_io_async::Read {
    pub async fn new(connection: &'a mut T) -> Result<Self, Error>
    {
        let mut client = HTTP2Client {
            connection,
            hpack_encoder: hpack::Encoder::new(),
            hpack_decoder: hpack::Decoder::new(),
            largest_stream_id: 1
        };
        client.open_connection().await?;
        Ok(client)
    }

    pub async fn open_connection(&mut self) -> Result<(), Error>
    {
        self.connection.write_all(HTTP2_CONNECTION_PREFACE.as_bytes()).await.map_err(|_| Error::WriteError)?;

        self.write_frame(Frame::Settings::<0>(SettingsFrame::new(false))).await?;

        let mut has_acked_ours = false;

        // Wait for server to send it's settings frame
        let mut rx_buffer = [0u8; 200];
        loop {
            let new_frame = self.read_frame(&mut rx_buffer).await?;
            match new_frame {
                Frame::Settings(settings_frame) => {
                    if settings_frame.ack {
                        has_acked_ours = true;
                    }
                    else {
                        break;
                    }

                }
                _ => {}
            }
        }

        // Send our own ack to their settings
        self.write_frame(Frame::Settings::<0>(SettingsFrame::new(true))).await?;

        // Wait for them to ack ours
        if !has_acked_ours {
            loop {
                let new_frame = self.read_frame(&mut rx_buffer).await?;
                match new_frame {
                    Frame::Settings(settings_frame) => {
                        if settings_frame.ack {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    pub async fn write_frame<const L: usize>(&mut self, frame: Frame<'_, L>) -> Result<(), Error>
        where
            [(); {L+1}]:
    {
        let mut header_buff = [0u8; 9];
        let mut misc_buff = [0u8; 4];
        let header_frame_slices = frame.encode_frame(&mut header_buff, &mut misc_buff)?;
        for slice in header_frame_slices.to_slice_array() {
            self.connection.write_all(slice).await.map_err(|_| Error::WriteError)?;
        }
        Ok(())
    }

    pub async fn read_frame<'b>(&mut self, buffer: &'b mut [u8]) -> Result<Frame<'b, 1>, Error>{
        // Read the frame header
        self.connection.read_exact(&mut buffer[0..9]).await.map_err(|_| Error::ReadError)?;
        let payload_len: usize = (buffer[0] as usize) << 16 | (buffer[1] as usize) << 8 | (buffer[2] as usize) << 0;
        // Read the frame payload
        self.connection.read_exact(&mut buffer[9..9+payload_len]).await.map_err(|_| Error::ReadError)?;
        Frame::decode_frame(buffer)
    }

    pub async fn lossy_receive_loop(&mut self) -> ! {
        let mut buffer = [0u8; 256];
        loop {
            match self.connection.read(&mut buffer).await {
                Ok(_) => {}
                Err(_) => {}
            };
        }
    }

    pub async fn new_outbound_stream<const L: usize>(&mut self, header_list: PlaintextHeaderList<'_, L>) -> Result<HTTP2Stream, Error> {
        // Client-initiated streams must be odd
        let new_stream_id = if self.largest_stream_id%2 == 0 {
            self.largest_stream_id + 1
        }
        else {
            self.largest_stream_id + 2
        };

        let mut raw_headers = [0u8; 200];
        let encoded_headers = self.hpack_encoder.encode_header_list(header_list, &mut raw_headers);
        let header_frame = Frame::<1>::Headers(HeaderFrame::new(new_stream_id, false, true, encoded_headers));
        self.write_frame(header_frame).await?;

        self.largest_stream_id = new_stream_id;

        Ok(HTTP2Stream {
            stream_id: new_stream_id
        })
    }

    pub async fn send_data<'b, const L: usize>(&mut self, stream: &HTTP2Stream, data: BufferSlice<'b, L>, end_stream: bool) -> Result<(), Error>
    where
        [(); {L+1}]:
    {
        let new_frame = Frame::Data(
            DataFrame::new(
                stream.stream_id,
                end_stream,
                data
            )
        );
        self.write_frame(new_frame).await
    }
}

pub struct HTTP2Stream {
    stream_id: u32
}