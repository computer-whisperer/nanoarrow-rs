use alloc::vec::Vec;
use core::marker::PhantomData;
use core::ops::Deref;
use embassy_futures::select::select_slice;
use embassy_sync::blocking_mutex::raw::RawMutex;
use embassy_sync::mutex::{MappedMutexGuard, Mutex, MutexGuard, TryLockError};
use embassy_sync::signal::Signal;
use flatbuffers::FlatBufferBuilder;
use crate::buffer_slice::BufferSlice;
use crate::flight::{build_path_descriptor, encode_record_batch_from_parts};
use crate::grpc::{GRPCCompressor, GRPCMessageBox, GRPCMessageBoxWritable};
use crate::time_series_record_batch::{self, RecordBatch, RecordBatchSerializable};

pub trait RecordBatchSwapchainExportable<RawMutexType>
where
    RawMutexType: RawMutex,
{
    fn get_schema<'a, 'fbb>(&self, builder: &'a mut FlatBufferBuilder<'fbb>) -> &'a [u8];
    fn write_compressed_flight_record_batch_message<'a, 'fbb>(
        &self, builder: &'a mut FlatBufferBuilder<'fbb>, compressor: &mut GRPCCompressor, message_box: &mut dyn GRPCMessageBoxWritable)
        -> Result<(), TryLockError>;
    fn write_uncompressed_flight_record_batch_message<'a, 'fbb>(
        &self, builder: &'a mut FlatBufferBuilder<'fbb>, message_box: &mut dyn GRPCMessageBoxWritable)
        -> Result<(), TryLockError>;
    fn get_new_readable_signal(&self) -> &Signal<RawMutexType, ()>;
    fn get_path(&self) -> &[&str];
}

pub struct RecordBatchSwapchain<'buffer, 'a, RawMutexType, T>
where
    RawMutexType: RawMutex,
    T: RecordBatch<'buffer>,
{
    buffers: [Mutex<RawMutexType, T>; 2],
    currently_writable_index: Mutex<RawMutexType, usize>,
    currently_readable_index: Mutex<RawMutexType, usize>,
    new_readable_signal: Signal<RawMutexType, ()>,
    descriptor_path: &'a [&'a str],
    phantom: PhantomData<&'buffer ()>
}

impl<'buffer, 'a, RawMutexType, T> RecordBatchSwapchain<'buffer, 'a, RawMutexType, T>
where
    RawMutexType: RawMutex,
    T: RecordBatch<'buffer>,
{
    pub fn new(buffer: &'buffer mut [u8], descriptor_path: &'a [&'a str]) -> Self {
        let (buffer_a, buffer_b) = buffer.split_at_mut(buffer.len() / 2);
        Self {
            buffers: [
                Mutex::new(T::new_empty(buffer_a)),
                Mutex::new(T::new_empty(buffer_b))
            ],
            currently_writable_index: Mutex::new(0),
            currently_readable_index: Mutex::new(1),
            new_readable_signal: Signal::new(),
            descriptor_path,
            phantom: PhantomData::default()
        }
    }

    pub async fn append_row(&self, row: T::RowType) {
        let mut writable_index = self.currently_writable_index.lock().await;
        let mut writable_buffer = self.buffers[*writable_index].lock().await;
        match writable_buffer.append_row(row) {
            Ok(_) => {}
            Err(time_series_record_batch::Error::RecordBatchFullError) => {
                let mut readable_index = self.currently_readable_index.lock().await;
                *readable_index = *writable_index;
                drop(readable_index);
                self.new_readable_signal.signal(());
                *writable_index = (*writable_index + 1) % self.buffers.len();
                let mut writable_buffer = self.buffers[*writable_index].lock().await;
                writable_buffer.clear();
                writable_buffer.append_row(row).unwrap();
            }
        }
    }

    pub fn try_append_row(&self, row: T::RowType) {
        let mut writable_index = match self.currently_writable_index.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError) => return,
        };
        let mut writable_buffer = match self.buffers[*writable_index].try_lock() {
            Ok(guard) => guard,
            Err(TryLockError) => return,
        };
        match writable_buffer.append_row(row) {
            Ok(_) => {}
            Err(time_series_record_batch::Error::RecordBatchFullError) => return
        }
    }

    async fn get_readable_buffer(&self) -> MutexGuard<RawMutexType, T> {
        let readable_index = self.currently_readable_index.lock().await;
        let ret = self.buffers[*readable_index].lock().await;
        drop(readable_index);
        ret
    }

    fn try_get_readable_buffer(&self) -> Result<MutexGuard<RawMutexType, T>, TryLockError> {
        let readable_index = match self.currently_readable_index.try_lock() {
            Ok(x) => x,
            Err(x) => return Err(x)
        };
        let ret = match self.buffers[*readable_index].try_lock() {
            Ok(guard) => guard,
            Err(TryLockError) => return Err(TryLockError),
        };
        drop(readable_index);
        Ok(ret)
    }

    fn try_get_any_buffer(&self) -> Result<MutexGuard<RawMutexType, T>, TryLockError> {
        match self.buffers[0].try_lock() {
            Ok(guard) => Ok(guard),
            Err(TryLockError) => match self.buffers[1].try_lock() {
                Ok(guard) => Ok(guard),
                Err(TryLockError) => Err(TryLockError),
            }
        }
    }

}

impl <'buffer, 'a, RawMutexType, T> RecordBatchSwapchainExportable<RawMutexType> for RecordBatchSwapchain<'buffer, 'a, RawMutexType, T>
where
    RawMutexType: RawMutex,
    T: RecordBatch<'buffer>,
{

    fn get_schema<'b, 'fbb>(&self, builder: &'b mut FlatBufferBuilder<'fbb>) -> &'b [u8]
    {
        T::get_schema(builder)
    }

    fn write_compressed_flight_record_batch_message<'b, 'fbb>(
        &self, builder: &'b mut FlatBufferBuilder<'fbb>, compressor: &mut GRPCCompressor, message_box: &mut dyn GRPCMessageBoxWritable)
        -> Result<(), TryLockError> {
        let buffer = self.try_get_readable_buffer()?;
        let (header, body) = buffer.get_record_batch(builder);
        let descriptor= build_path_descriptor(self.descriptor_path);
        let mut temp_buffer = [0u8; 200];
        let raw_message = encode_record_batch_from_parts(descriptor, &mut temp_buffer, header, body);
        let (proto_body, is_compressed) = raw_message.get_proto_message();
        assert!(!is_compressed);
        message_box.compress_from_slice_slice(compressor, proto_body.to_slice_slice());
        Ok(())
    }

    fn write_uncompressed_flight_record_batch_message<'b, 'fbb>(
        &self, builder: &'b mut FlatBufferBuilder<'fbb>, message_box: &mut dyn GRPCMessageBoxWritable)
        -> Result<(), TryLockError> {
        let buffer = self.try_get_readable_buffer()?;
        let (header, body) = buffer.get_record_batch(builder);
        let descriptor= build_path_descriptor(self.descriptor_path);
        let mut temp_buffer = [0u8; 200];
        let raw_message = encode_record_batch_from_parts(descriptor, &mut temp_buffer, header, body);
        let (proto_body, is_compressed) = raw_message.get_proto_message();
        assert!(!is_compressed);
        message_box.copy_from_slice_slice(proto_body.to_slice_slice());
        Ok(())
    }

    fn get_new_readable_signal(&self) -> &Signal<RawMutexType, ()> {
        &self.new_readable_signal
    }

    fn get_path(&self) -> &[&str] {
        self.descriptor_path
    }
}

mod test {
    use embassy_sync::blocking_mutex::raw::NoopRawMutex;
    use flatbuffers::FlatBufferBuilder;
    use crate::buffer_slice::BufferSlice;
    use crate::grpc::{GRPCCompressor, GRPCMessageBox};
    use crate::record_batch_swapchain::{RecordBatchSwapchain, RecordBatchSwapchainExportable};
    use crate::time_series_record_batch::{RecordBatch, TimeSeriesRecordBatch};

    type ProjectMutex = NoopRawMutex;

    #[test]
    fn test()
    {
        let mut raw_data_1 = [0u8; 16000];
        let mut record_batch_swapchain = RecordBatchSwapchain::<ProjectMutex, TimeSeriesRecordBatch<u64, f32>>::new(
            &mut raw_data_1,
            &["test1"]
        );

        let record_batch_swapchain_receiver: &dyn RecordBatchSwapchainExportable<ProjectMutex> = &record_batch_swapchain;

        for i in 0..1000 {
            record_batch_swapchain.try_append_row((1.0, i));
        }

        let mut builder = FlatBufferBuilder::new();
        let mut compressor = GRPCCompressor::new();
        let mut mailbox = GRPCMessageBox::<1000>::new();
        record_batch_swapchain_receiver.write_compressed_flight_record_batch_message(&mut builder, &mut compressor, &mut mailbox).unwrap()
    }
}