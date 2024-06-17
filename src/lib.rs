
#![no_std]
#![feature(generic_const_exprs)]

extern crate alloc;


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

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;

    #[test]
    fn it_works() {



    }
}
