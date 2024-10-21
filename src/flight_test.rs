#![feature(generic_const_exprs)]

extern crate alloc;

use std::net::SocketAddr;
use embedded_io_async::Write;
use std_embedded_nal_async;

use crate::time_series_record_batch::TimeSeriesRecordBatch;
use crate::time_series_record_batch::RecordBatch;
use embedded_nal_async::{Dns, TcpConnect};
use femtopb::{Message, repeated};
use crate::grpc::{GRPCClient, GRPCCompressor};
use crate::flight::{FlightClient};
use embassy_sync::channel::Channel;
use embassy_futures::join::{join, join3};

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
mod record_batch_swapchain;

async fn run<S, E, DnsError>(stack: &mut S) -> Result<(), E>
    where
        E: core::fmt::Debug, // Might go away when MSRV goes up to 1.49, see https://github.com/rust-lang/rust/issues/80821
        DnsError: core::fmt::Debug,
        S: TcpConnect<Error = E> + Dns<Error = DnsError>,
{
    let target = SocketAddr::new(
        stack.get_host_by_name("localhost", embedded_nal_async::AddrType::IPv6).await.unwrap(),
        8815
    );


    type RawMutexType = NoopRawMutex;

    let mut flight_client = FlightClient::<RawMutexType, 10000, 3>::new();

    let mut raw_buf_a = [0u8;   1000];
    let mut raw_buf_b = [0u8; 10000];

    let input_a = RecordBatchSwapchain::<RawMutexType, TimeSeriesRecordBatch<u64, f32>>::new(&mut raw_buf_a[..], &["input_a"]);

    let input_b = RecordBatchSwapchain::<RawMutexType, TimeSeriesRecordBatch<u64, f32>>::new(&mut raw_buf_b[..], &["input_b"]);

    flight_client.add_swapchain_export(&input_a);
    flight_client.add_swapchain_export(&input_b);

    let writer_future = async {
        for i in 0..50000 {
            input_a.append_row((0.0f32, i)).await;
            input_b.append_row((1.0f32, i)).await;
            Timer::after_millis(1).await;
        }
        print!("Done appending rows\r\n");
    };

    //let processing_future = flight_client.compression_loop();
    let processing_future = flight_client.copy_loop();
    let grpc_future = flight_client.grpc_loop(stack, target);

    join3(writer_future, processing_future, grpc_future).await;

    Ok(())
}

use embassy_executor::Spawner;
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_time::Timer;
use crate::record_batch_swapchain::RecordBatchSwapchain;

#[embassy_executor::main]
async fn main(spawner: Spawner) ->! {


    let mut stack = std_embedded_nal_async::Stack::default();

    run(&mut stack).await.unwrap();

    loop {}
}