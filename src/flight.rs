use alloc::vec;
use femtopb::{item_encoding, Message, repeated};
use femtopb::encoding::WireType;
use crate::buffer_slice::BufferSlice;
use crate::http2;
use crate::time_series_record_batch::{RecordBatch, TimeSeriesRecordBatch};
use crate::grpc::{GRPCClient, GRPCMessage, GRPCCall};

#[derive(Clone, femtopb::Message)]
pub struct FlightDescriptor<'a> {
    #[femtopb(uint32, tag = 1)]
    pub descriptor_type: u32,
    #[femtopb(bytes, tag = 2)]
    pub cmd: &'a [u8],
    #[femtopb(string, repeated, tag = 3)]
    pub path: repeated::Repeated<'a, &'a str, item_encoding::String>,
    #[femtopb(unknown_fields)]
    pub unknown_fields: femtopb::UnknownFields<'a>
}

#[derive(Clone, femtopb::Message)]
pub struct FlightData<'a> {
    #[femtopb(message, tag = 1)]
    pub flight_descriptor: Option<FlightDescriptor<'a>>,
    #[femtopb(bytes, tag = 2)]
    pub data_header: &'a [u8],
    #[femtopb(bytes, tag = 3)]
    pub app_metadata: &'a [u8],
    #[femtopb(bytes, tag = 1000)]
    pub data_body: &'a [u8],
    #[femtopb(unknown_fields)]
    pub unknown_fields: femtopb::UnknownFields<'a>
}

pub fn encode_flight_data_with_body<'a, const L: usize>(
    mut flight_data: FlightData,
    flight_data_body: BufferSlice<'a, L>,
    buffer: &'a mut [u8]) -> GRPCMessage<'a, {L + 1}> {
    flight_data.data_body = &[];

    let new_len = {
        let buffer_len = buffer.len();
        let mut buf_out = &mut buffer[..];
        flight_data.encode(&mut buf_out).unwrap();
        femtopb::encoding::encode_key(1000, WireType::LengthDelimited, &mut buf_out);
        femtopb::encoding::encode_varint(flight_data_body.len() as u64, &mut buf_out);
        buffer_len - buf_out.len()
    };

    let result_buffer_slice = flight_data_body.prepend_slice(&buffer[0..new_len]);
    GRPCMessage::from_proto_message(result_buffer_slice, false)
}

pub fn encode_flight_data<'a>(mut flight_data: FlightData, buffer: &'a mut [u8]) -> GRPCMessage<'a, 1> {
    let bytes_written = {
        let buffer_len = buffer.len();
        let mut buf_out = &mut buffer[..];
        flight_data.encode(&mut buf_out).unwrap();
        buffer_len - buf_out.len()
    };

    let result_buffer_slice = BufferSlice::new_from_slice(&buffer[0..bytes_written]);
    GRPCMessage::from_proto_message(result_buffer_slice, false)
}

pub fn encode_schema<'a, const L: usize, T: RecordBatch<L>> (
    record_batch: &'a mut T,
    flight_descriptor: FlightDescriptor<'_>,
    buffer: &'a mut [u8]
) -> GRPCMessage<'a, 1> {
    let schema_bin = record_batch.get_binary_schema();

    let flight_data = FlightData{
        flight_descriptor: Some(flight_descriptor),
        data_header: schema_bin,
        .. Default::default()
    };
    encode_flight_data(flight_data, buffer)
}

pub fn encode_record_batch<'a, const L: usize, T: RecordBatch<L>> (
    record_batch: &'a mut T,
    flight_descriptor: FlightDescriptor<'_>,
    buffer: &'a mut [u8]) -> GRPCMessage<'a, { L + 1 }> {

    let (record_batch_header, record_batch_body_slices) = record_batch.get_binary_record_batch();

    let flight_data = FlightData{
        flight_descriptor: Some(flight_descriptor),
        data_header: record_batch_header,
        .. Default::default()
    };

    encode_flight_data_with_body(flight_data, record_batch_body_slices, buffer)
}

pub struct FlightClient<'a, T>
where
    T: embedded_io_async::Write + embedded_io_async::Read,
{
    grpc_client: GRPCClient<'a, T>
}

impl<'a, T> FlightClient<'a, T>
where
    T: embedded_io_async::Write + embedded_io_async::Read,
{
    pub async fn new(connection: &'a mut T) -> Result<Self, http2::Error> {
        Ok(Self {
            grpc_client: GRPCClient::new(connection).await?
        })
    }

    pub async fn do_put(&mut self) -> Result<FlightDoPut, http2::Error> {
        let grpc_call = self.grpc_client.new_call("/arrow.flight.protocol.FlightService/DoPut").await?;
        Ok(FlightDoPut {
            grpc_call
        })
    }
}

struct FlightDoPut {
    grpc_call: GRPCCall
}

mod tests {
    extern crate std;

    fn test() {

    }
}