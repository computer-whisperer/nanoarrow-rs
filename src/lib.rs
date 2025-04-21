#![no_std]
#![feature(generic_const_exprs)]

extern crate alloc;

mod scalar_array;
pub mod time_series_record_batch;
#[allow(non_snake_case)]
mod Schema_generated;
#[allow(non_snake_case)]
mod Tensor_generated;
#[allow(non_snake_case)]
mod SparseTensor_generated;
#[allow(non_snake_case)]
mod Message_generated;
pub mod flight;
mod http2;
mod hpack;
mod grpc;
mod buffer_slice;
pub mod record_batch_swapchain;
mod zlib;
mod deflate;

pub use flight::FlightClient;
pub use record_batch_swapchain::RecordBatchSwapchain;
pub use time_series_record_batch::{RecordBatch, TimeSeriesRecordBatch};
pub use scalar_array::ArrowPrimitive;