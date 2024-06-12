#![feature(generic_const_exprs)]

extern crate alloc;

use embedded_io_async::Write;
use std_embedded_nal_async;

use crate::time_series_record_batch::TimeSeriesRecordBatch;

use embedded_nal_async::{Dns, TcpConnect};
use femtopb::{Message, repeated};
use crate::grpc::GRPCClient;
use crate::flight::FlightClient;

mod scalar_array;
mod time_series_record_batch;
mod Schema_generated;
mod Tensor_generated;
mod SparseTensor_generated;
mod Message_generated;
mod flight;
mod http2;
mod hpack;
mod grpc;
mod buffer_slice;

async fn run<S, E, DnsError>(stack: &mut S) -> Result<(), E>
    where
        E: core::fmt::Debug, // Might go away when MSRV goes up to 1.49, see https://github.com/rust-lang/rust/issues/80821
        DnsError: core::fmt::Debug,
        S: TcpConnect<Error = E> + Dns<Error = DnsError>,
{
    let target = embedded_nal_async::SocketAddr::new(
        stack.get_host_by_name("localhost", embedded_nal_async::AddrType::IPv6).await.unwrap(),
        8815
    );

    print!("Starting Connection.\n");
    let mut connection = stack.connect(target).await.unwrap();

    let mut grpc_client = GRPCClient::new(&mut connection).await.unwrap();
    //let mut flight_client = FlightClient::new(&mut connection).await.unwrap();

    let mut raw_data_1 = [0u8; 16000];
    let mut time_series_test_1 = TimeSeriesRecordBatch::new_empty(&mut raw_data_1);
    for i in 0..1000 {
        time_series_test_1.append(1.0, i);
    }

    let mut raw_data_2 = [0u8; 16000];
    let mut time_series_test_2 = TimeSeriesRecordBatch::new_empty(&mut raw_data_2);
    for i in 0..1000 {
        time_series_test_2.append(2.0, i);
    }

    let raw_path_1 = ["test1"];

    let flight_descriptor_1 = flight::FlightDescriptor{
        descriptor_type: 1,
        path: repeated::Repeated::from_slice(&raw_path_1),
        .. Default::default()
    };

    let raw_path_2 = ["test2"];

    let flight_descriptor_2 = flight::FlightDescriptor{
        descriptor_type: 1,
        path: repeated::Repeated::from_slice(&raw_path_2),
        .. Default::default()
    };

    let mut serialize_buffer = [0u8; 500];

    //let mut do_put_1 = flight_client.do_put().await.unwrap();

    //let mut do_put_2 = flight_client.do_put().await.unwrap();

    let mut grpc_call_1 = grpc_client.new_call("/arrow.flight.protocol.FlightService/DoPut").await.unwrap();

    print!("Sending schema 1\n");
    let schema_msg = flight::encode_schema(&mut time_series_test_1, flight_descriptor_1.clone(), &mut serialize_buffer);
    grpc_client.send_message(&grpc_call_1, schema_msg, false).await.unwrap();

    let mut grpc_call_2 = grpc_client.new_call("/arrow.flight.protocol.FlightService/DoPut").await.unwrap();
    print!("Sending schema 2\n");
    let schema_msg = flight::encode_schema(&mut time_series_test_2, flight_descriptor_2.clone(), &mut serialize_buffer);
    grpc_client.send_message(&grpc_call_2, schema_msg, false).await.unwrap();

    print!("Sending record batch 1\n");
    let record_batch_msg = flight::encode_record_batch(&mut time_series_test_1, flight_descriptor_1.clone(), &mut serialize_buffer);
    grpc_client.send_message(&grpc_call_1, record_batch_msg, false).await.unwrap();

    print!("Sending record batch 2\n");
    let record_batch_msg = flight::encode_record_batch(&mut time_series_test_2, flight_descriptor_2.clone(), &mut serialize_buffer);
    grpc_client.send_message(&grpc_call_2, record_batch_msg, false).await.unwrap();

    print!("Listening\n");
    let mut rx_buffer = [0u8; 2000];
    loop {
        let new_frame = grpc_client.http2_client.read_frame(&mut rx_buffer).await.unwrap();
        print!("{:?}\n", new_frame);
    }

    Ok(())
}

#[async_std::main]
async fn main() {


    let mut stack = std_embedded_nal_async::Stack::default();

    run(&mut stack).await.unwrap()
}