use alloc::vec;
use alloc::vec::Vec;
use miniz_oxide::deflate::core::{
    compress,
    CompressorOxide,
    TDEFLFlush,
    TDEFLStatus,};

pub fn zlib_deflate<'b>(slices: &[&[u8]], compressor: &mut CompressorOxide, buffer: &'b mut [u8]) -> &'b [u8] {
    compressor.reset();
    let mut bytes_written = 0;
    for i in 0..slices.len() {
        let flush_mode = if i < slices.len()-1 {TDEFLFlush::Sync} else {TDEFLFlush::Finish};
        let (status, input_pos, output_pos) = compress(
            compressor,
            slices[i],
            &mut buffer[bytes_written..],
            flush_mode);
        bytes_written += output_pos;
        assert_eq!(input_pos, slices[i].len());
        assert_eq!(status, match flush_mode {TDEFLFlush::Finish => TDEFLStatus::Done, _ => TDEFLStatus::Okay});
    }

    &buffer[..bytes_written]
}


#[derive(Debug, Clone)]
pub struct BufferSlice<'a, const L: usize>
{
    data: [&'a [u8]; L]
}

impl<'a, const L: usize> BufferSlice<'a, L> {
    pub fn new_from_slice_array(data: [&'a [u8]; L]) -> Self {
        Self {
            data
        }
    }

    pub fn to_slice_array(&self) -> [&'a [u8]; L] {
        self.data
    }

    pub fn to_slice_slice(&self) -> &[&'a [u8]] {
        &self.data[..]
    }

    pub fn append_slice(&self, data: &'a [u8]) -> BufferSlice<'a, { L + 1 }> {
        let mut new_slice: [&[u8]; L+1] = [&[]; L+1];
        new_slice[0..L].copy_from_slice(&self.data);
        new_slice[L] = data;
        BufferSlice::<'a, { L + 1 }>::new_from_slice_array(new_slice)
    }

    pub fn prepend_slice(&self, data: &'a [u8]) -> BufferSlice<'a, { L + 1 }> {
        let mut new_slice: [&[u8]; L+1] = [&[]; L+1];
        new_slice[1..L+1].copy_from_slice(&self.data);
        new_slice[0] = data;
        BufferSlice::<'a, { L + 1 }>::new_from_slice_array(new_slice)
    }

    pub fn split_at(&self, index: usize) -> (Self, Self) {
        // Returns two instances of BufferSlice, the first with the first `index` bytes, the second with the rest
        let mut first_slice: [&'a [u8]; L] = [&[]; L];
        let mut first_slice_idx = 0;
        let mut second_slice: [&'a [u8]; L] = [&[]; L];
        let mut second_slice_idx = 0;
        let mut bytes_so_far = 0;
        for slice_index in 0..L {
            let this_len = self.data[slice_index].len();
            if bytes_so_far > index {
                second_slice[second_slice_idx] = self.data[slice_index];
                second_slice_idx += 1;
            }
            else if bytes_so_far + this_len < index {
                first_slice[first_slice_idx] = self.data[slice_index];
                first_slice_idx += 1;
            }
            else {
                let index_in_slice = index - bytes_so_far;
                first_slice[first_slice_idx] = &self.data[slice_index][0..index_in_slice];
                first_slice_idx += 1;
                second_slice[second_slice_idx] = &self.data[slice_index][index_in_slice..];
                second_slice_idx += 1;
            }
            bytes_so_far += this_len;
        }

        (Self::new_from_slice_array(first_slice), Self::new_from_slice_array(second_slice))
    }

    pub fn len(&self) -> usize {
        let mut len = 0;
        for slice in self.data {
            len += slice.len();
        }
        len
    }

    pub fn zlib_deflate<'b>(&self, compressor: &mut CompressorOxide, buffer: &'b mut [u8]) -> &'b [u8] {
        zlib_deflate(self.to_slice_slice(), compressor, buffer)
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut vec = vec![];
        for slice in self.data {
            vec.extend_from_slice(slice);
        }
        vec
    }
}

impl<'a> BufferSlice<'a, 1> {
    pub fn new_from_slice(data: &'a [u8]) -> Self {
        let inner_data = [data];
        Self {
            data: inner_data
        }
    }
}

impl<'a, const L: usize> core::ops::Index<usize> for BufferSlice<'a, L> {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        let mut index = index;
        let mut slice_index = 0;
        loop {
            if index < self.data[slice_index].len() {
                return &self.data[slice_index][index];
            }
            index -= self.data[slice_index].len();
            slice_index += 1;
        }
    }
}

impl<'a, const L: usize> core::ops::Index<core::ops::Range<usize>> for BufferSlice<'a, L> {
    type Output = BufferSlice<'a, L>;

    fn index(&self, index: core::ops::Range<usize>) -> &Self::Output {
        unimplemented!()
    }
}