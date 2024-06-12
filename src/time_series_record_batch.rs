use alloc::vec;
use alloc::vec::Vec;
use flatbuffers::FlatBufferBuilder;
use crate::buffer_slice::BufferSlice;
use crate::Message_generated::org::apache::arrow::flatbuf::{self, Message, MessageArgs, RecordBatchArgs, FieldNode, MessageHeader};
use crate::scalar_array::BackedMutableScalarArray;
use crate::Schema_generated::org::apache::arrow::flatbuf::{Schema, SchemaArgs, Endianness, Field, FieldArgs, Buffer, MetadataVersion};
use crate::scalar_array::ArrowPrimitive;

pub trait RecordBatch<const L: usize> {
    fn get_binary_schema(&self) -> &[u8];
    fn get_binary_record_batch(&mut self) -> (&[u8], BufferSlice<L>);
}

pub struct TimeSeriesRecordBatch<'a, 'b, TimeType: ArrowPrimitive, ValueType: ArrowPrimitive>
{
    time_array: BackedMutableScalarArray<'a, TimeType>,
    value_array: BackedMutableScalarArray<'a, ValueType>,
    schema_builder: FlatBufferBuilder<'b>,
    record_batch_builder: FlatBufferBuilder<'b>
}

impl<'a, 'b, TimeType: ArrowPrimitive, ValueType: ArrowPrimitive> TimeSeriesRecordBatch<'a, 'b, TimeType, ValueType>
{
    pub fn new_empty(raw_data: &'a mut [u8]) -> Self {
        let bytes_per_entry = core::mem::size_of::<TimeType>() + core::mem::size_of::<ValueType>();
        let max_len = ((raw_data.len()/8) / bytes_per_entry)*8;
        let (time_data, value_data) = raw_data.split_at_mut(max_len * core::mem::size_of::<TimeType>());
        Self {
            time_array: BackedMutableScalarArray::new_empty_from_u8(time_data),
            value_array: BackedMutableScalarArray::new_empty_from_u8(value_data),
            schema_builder: Self::build_schema("value"),
            record_batch_builder: FlatBufferBuilder::new(),
        }
    }

    pub fn append(&mut self, value: ValueType, time: TimeType) {
        self.time_array.append(time).unwrap();
        self.value_array.append(value).unwrap();
    }

    fn build_schema(value_name: &str) -> FlatBufferBuilder<'b> {
        let mut builder = FlatBufferBuilder::new();
        let time_name = builder.create_string("time");
        let arrow_type = TimeType::arrow_type(&mut builder);
        let time_field = Field::create(&mut builder, &FieldArgs{
            name: Some(time_name),
            nullable: false,
            type_type: TimeType::arrow_type_type(),
            type_: Some(arrow_type),
            dictionary: None,
            children: None,
            custom_metadata: None,
        });
        let value_name = builder.create_string(value_name);
        let arrow_type = ValueType::arrow_type(&mut builder);
        let value_field = Field::create(&mut builder, &FieldArgs{
            name: Some(value_name),
            nullable: false,
            type_type: ValueType::arrow_type_type(),
            type_: Some(arrow_type),
            dictionary: None,
            children: None,
            custom_metadata: None,
        });
        let fields = builder.create_vector(&[time_field, value_field]);
        let schema = Schema::create(&mut builder, &SchemaArgs{
            endianness: Endianness::Little,
            fields: Some(fields),
            custom_metadata: None,
            features: None,
        });
        let message = Message::create(&mut builder, &MessageArgs{
            version: MetadataVersion::V5,
            header_type: MessageHeader::Schema,
            header: Some(schema.as_union_value()),
            bodyLength: 0,
            custom_metadata: None
        });
        builder.finish(message, None);
        return builder;
    }


}

impl <'a, 'b, TimeType: ArrowPrimitive, ValueType: ArrowPrimitive> RecordBatch<2> for TimeSeriesRecordBatch<'a, 'b, TimeType, ValueType> {

    fn get_binary_schema(&self) -> &[u8] {
        self.schema_builder.finished_data()
    }

    fn get_binary_record_batch(&mut self) -> (&[u8], BufferSlice<2>) {
        let time_slice = self.time_array.as_aligned_slice();
        let value_offset = time_slice.len();
        let value_slice = self.value_array.as_aligned_slice();

        let num_rows = self.time_array.len().min(self.value_array.len());

        self.record_batch_builder.reset();
        let nodes = self.record_batch_builder.create_vector(
            &[
                FieldNode::new(self.time_array.len() as i64, 0),
                FieldNode::new(self.value_array.len() as i64, 0),
            ]
        );
        let buffers = self.record_batch_builder.create_vector(
            &[
                Buffer::new(0, 0), // Time validity
                Buffer::new(0, self.time_array.len_in_bytes() as i64),
                Buffer::new(0, 0), // Offset Validity
                Buffer::new(value_offset as i64, self.value_array.len_in_bytes() as i64),
            ]
        );
        let record_batch = flatbuf::RecordBatch::create(&mut self.record_batch_builder, &RecordBatchArgs{
            length: num_rows as i64,
            nodes: Some(nodes),
            buffers: Some(buffers),
            compression: None,
            variadicBufferCounts: None,
        });
        let message = Message::create(&mut self.record_batch_builder, &MessageArgs{
            version: MetadataVersion::V5,
            header_type: MessageHeader::RecordBatch,
            header: Some(record_batch.as_union_value()),
            bodyLength: (time_slice.len() + value_slice.len()) as i64,
            custom_metadata: None
        });
        self.record_batch_builder.finish(message, None);

        let record_batch_slice = self.record_batch_builder.finished_data();
        (record_batch_slice, BufferSlice::new_from_slice_array([time_slice, value_slice]))
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use alloc::vec;
    use alloc::vec::Vec;
    use std::sync::Arc;
    use arrow::buffer::Buffer;
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::ipc::{MetadataVersion, reader};
    use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
    use arrow_flight::{FlightData, FlightDescriptor, FlightInfo};
    use arrow_flight::utils::flight_data_to_arrow_batch;
    use crate::time_series_record_batch::{RecordBatch, TimeSeriesRecordBatch};

    #[test]
    fn basic_test() {
        let mut raw_data = [0u8; 100];
        let mut time_series_test = TimeSeriesRecordBatch::new_empty(&mut raw_data);
        time_series_test.append(5f32, 5u32);
        let schema_bin = Vec::from(time_series_test.get_binary_schema());

        let mut record_batch_body = vec![];
        let (record_batch_header, record_batch_body_slices) = time_series_test.get_binary_record_batch();
        for slice in record_batch_body_slices {
            record_batch_body.extend_from_slice(slice);
        }

        // Build record batch with our library
        let mut raw_data = [0u8; 100];
        let mut time_series_test = TimeSeriesRecordBatch::new_empty(&mut raw_data);
        time_series_test.append(5f32, 5u32);
        time_series_test.append(123f32, 444u32);
        let schema_bin = Vec::from(time_series_test.get_binary_schema());

        let mut record_batch_body = vec![];
        let (record_batch_header, record_batch_body_slices) = time_series_test.get_binary_record_batch();
        for slice in record_batch_body_slices {
            record_batch_body.extend_from_slice(slice);
        }

        // Decode with mainstream arrow
        let schema = arrow::ipc::convert::try_schema_from_flatbuffer_bytes(&schema_bin).unwrap();


        let message = arrow::ipc::root_as_message(&record_batch_header).unwrap();

        let dictionaries_by_id = std::collections::HashMap::new();

        let init_record_batch = message.header_as_record_batch().unwrap();
        let record_batch = reader::read_record_batch(
            &Buffer::from(&record_batch_body),
            init_record_batch,
            SchemaRef::from(schema),
            &dictionaries_by_id,
            None,
            &message.version(),
        ).unwrap();

        let time_data = record_batch.column_by_name("time").unwrap().to_data();
        let value_data = record_batch.column_by_name("value").unwrap().to_data();
        let time_col: &[u32] = time_data.buffer(0);
        let value_col: &[f32] = value_data.buffer(0);
        assert_eq!(time_col[0], 5);
        assert_eq!(value_col[0], 5f32);
    }
}