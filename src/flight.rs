use alloc::vec;
use alloc::vec::Vec;
use core::future::poll_fn;
use core::net::SocketAddr;
use core::ops::{Deref, DerefMut};
use core::task::Poll;
use embassy_futures::select::select_slice;
use embassy_sync::blocking_mutex::raw::RawMutex;
use embassy_sync::channel::Channel;
use embassy_sync::mutex::{Mutex, TryLockError};
use embassy_sync::signal::Signal;
use embedded_nal_async::{Dns, TcpConnect};
use femtopb::{item_encoding, Message, repeated};
use femtopb::encoding::WireType;
use flatbuffers::FlatBufferBuilder;
use defmt::info;
use crate::buffer_slice::BufferSlice;
use crate::http2;
use crate::time_series_record_batch::{RecordBatch, TimeSeriesRecordBatch};
use crate::grpc::{GRPCClient, GRPCMessage, GRPCCall, GRPCMessageBox, GRPCCompressor};
use crate::record_batch_swapchain::RecordBatchSwapchainExportable;

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

pub fn encode_schema<'a, T: RecordBatch<'a>> (
    record_batch: &'a mut T,
    flight_descriptor: FlightDescriptor<'_>,
    buffer: &'a mut [u8],
    builder: &mut FlatBufferBuilder
) -> GRPCMessage<'a, 1> {
    let schema_bin = T::get_schema(builder);

    let flight_data = FlightData{
        flight_descriptor: Some(flight_descriptor),
        data_header: schema_bin,
        .. Default::default()
    };
    encode_flight_data(flight_data, buffer)
}

pub fn build_path_descriptor<'a>(path: &'a[&'a str]) -> FlightDescriptor<'a> {
    FlightDescriptor{
        descriptor_type: 1,
        path: repeated::Repeated::from_slice(path),
        .. Default::default()
    }
}

pub fn build_schema_message_from_parts<'a>(raw: &'a [u8], enc_buffer: &'a mut [u8], path: &'a [&'a str]) -> GRPCMessage<'a, 1> {
    let descriptor = build_path_descriptor(path);
    let flight_data = FlightData{
        flight_descriptor: Some(descriptor),
        data_header: raw,
        .. Default::default()
    };
    encode_flight_data(flight_data, enc_buffer)
}

pub fn encode_record_batch<'a, T: RecordBatch<'a>> (
    record_batch: &'a mut T,
    flight_descriptor: FlightDescriptor<'_>,
    buffer: &'a mut [u8],
    builder: &mut FlatBufferBuilder) -> GRPCMessage<'a, 3> {
    let (record_batch_header, record_batch_body_slices) = record_batch.get_record_batch(builder);

    let flight_data = FlightData{
        flight_descriptor: Some(flight_descriptor),
        data_header: record_batch_header,
        .. Default::default()
    };

    encode_flight_data_with_body(flight_data, record_batch_body_slices, buffer)
}

pub fn encode_record_batch_from_parts<'a, const L: usize> (
    flight_descriptor: FlightDescriptor<'_>,
    buffer: &'a mut [u8],
    record_batch_header: &[u8],
    record_batch_body_slices: BufferSlice<'a, L>) -> GRPCMessage<'a, {L+1}>
    where
        [(); {L+1}]:
{

    let flight_data = FlightData{
        flight_descriptor: Some(flight_descriptor),
        data_header: record_batch_header,
        .. Default::default()
    };

    encode_flight_data_with_body(flight_data, record_batch_body_slices, buffer)
}

pub struct FlightGRPCReceiver<'sender, 'a, M, const RecordBatchLength: usize>
where
    M: RawMutex
{
    schema_message: GRPCMessage<'a, 1>,
    record_batch_message: &'sender Mutex<M, GRPCMessageBox<RecordBatchLength>>,
    grpc_call: Option<GRPCCall>
}

pub struct FlightClient<'a, MutexType, const BoxSize: usize, const BoxCount: usize>
where
    MutexType: RawMutex
{
    swapchain_exports: Vec<&'a dyn RecordBatchSwapchainExportable<MutexType>>,
    message_boxes: [Mutex<MutexType, GRPCMessageBox<BoxSize>>; BoxCount],
    ready_for_write: Channel<MutexType, usize, BoxCount>,
    ready_for_read: Channel<MutexType, (usize, usize), BoxCount>
}

impl <'a, MutexType, const BOX_SIZE: usize, const BOX_COUNT: usize> FlightClient<'a, MutexType, BOX_SIZE, BOX_COUNT>
where
    MutexType: RawMutex
{
    pub fn new() -> Self {
        let swapchain_exports = Vec::new();
        let message_boxes = core::array::from_fn(|_| Mutex::new(GRPCMessageBox::new()));
        let ready_for_write = Channel::new();
        let ready_for_read = Channel::new();
        for i in 0..BOX_COUNT {
            ready_for_write.try_send(i).unwrap();
        }
        Self {
            swapchain_exports,
            message_boxes,
            ready_for_write,
            ready_for_read
        }
    }

    pub fn add_swapchain_export<'b, 'c>(&'b mut self, export: &'c dyn RecordBatchSwapchainExportable<MutexType>)
    where
        'c: 'b,
        'c: 'a
    {
        self.swapchain_exports.push(export);
    }

    pub async fn compression_loop(&self) {
        let mut builder = FlatBufferBuilder::new();
        let mut compressor = GRPCCompressor::new();
        loop {
            let mut signal_futures = Vec::new();
            for s in &self.swapchain_exports {
                signal_futures.push(s.get_new_readable_signal().wait());
            }
            let (_, swapchain_idx) = select_slice(signal_futures.as_mut_slice()).await;
            let swapchain_exportable = self.swapchain_exports[swapchain_idx];
            let box_idx_to_write = self.ready_for_write.receive().await;
            let mut box_to_write = self.message_boxes[box_idx_to_write].lock().await;
            let box_ref = box_to_write.deref_mut();
            poll_fn(|cx| {
                match swapchain_exportable.write_compressed_flight_record_batch_message(&mut builder, &mut compressor, box_ref) {
                    Ok(_) => {
                        Poll::Ready(())
                    }
                    Err(_) => {
                        Poll::Pending
                    }
                }
            }).await;
            self.ready_for_read.send((box_idx_to_write, swapchain_idx)).await;
        }
    }

    pub async fn copy_loop(&self) {
        let mut builder = FlatBufferBuilder::new();
        loop {
            let mut signal_futures = Vec::new();
            for s in &self.swapchain_exports {
                signal_futures.push(s.get_new_readable_signal().wait());
            }
            let (_, swapchain_idx) = select_slice(signal_futures.as_mut_slice()).await;
            let swapchain_exportable = self.swapchain_exports[swapchain_idx];
            let box_idx_to_write = self.ready_for_write.receive().await;
            let mut box_to_write = self.message_boxes[box_idx_to_write].lock().await;
            let box_ref = box_to_write.deref_mut();
            poll_fn(|cx| {
                match swapchain_exportable.write_uncompressed_flight_record_batch_message(&mut builder, box_ref) {
                    Ok(_) => {
                        Poll::Ready(())
                    }
                    Err(_) => {
                        Poll::Pending
                    }
                }
            }).await;
            self.ready_for_read.send((box_idx_to_write, swapchain_idx)).await;
        }
    }

    pub async fn grpc_loop< E, ConnectType>(
        &self,
       tcp_connect: &ConnectType,
       address: SocketAddr,)
    where
        E: core::fmt::Debug,
        ConnectType: TcpConnect<Error = E>,
    {
        let mut connection = tcp_connect.connect(address).await.unwrap();

        let mut grpc_client = GRPCClient::new(&mut connection).await.unwrap();
        let mut builder = FlatBufferBuilder::new();

        let mut grpc_calls = Vec::<Option<GRPCCall>>::new();
        for _ in 0..self.swapchain_exports.len() {
            grpc_calls.push(None);
        }

        loop {
            let (box_to_read, swapchain_idx) = self.ready_for_read.receive().await;
            let grpc_call = match &grpc_calls[swapchain_idx] {
                None => {
                    let mut grpc_call = grpc_client.new_call("/arrow.flight.protocol.FlightService/DoPut").await.unwrap();
                    let raw_schema = self.swapchain_exports[swapchain_idx].get_schema(&mut builder);
                    let path = self.swapchain_exports[swapchain_idx].get_path();
                    let mut enc_buffer = [0u8; 200];
                    let message = build_schema_message_from_parts(raw_schema, & mut enc_buffer[..], path);
                    grpc_client.send_message(&mut grpc_call, &message, false).await.unwrap();
                    grpc_calls[swapchain_idx] = Some(grpc_call);
                    grpc_calls[swapchain_idx].as_ref().unwrap()
                }
                Some(x) => {x}
            };
            let message_box = self.message_boxes[box_to_read].lock().await;
            info!("Did send!");
            grpc_client.send_message(grpc_call, &message_box.get_message(), false).await.unwrap();
            self.ready_for_write.send(box_to_read).await;
        }
    }
}
