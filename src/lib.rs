
#![no_std]
#![feature(generic_const_exprs)]

extern crate alloc;


mod scalar_array;
pub mod time_series_record_batch;
mod Schema_generated;
mod Tensor_generated;
mod SparseTensor_generated;
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

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;

    #[test]
    fn it_works() {



    }
}
