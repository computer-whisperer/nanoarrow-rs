use core::marker::PhantomData;
use core::ops::Index;
use core::ptr::slice_from_raw_parts_mut;
use flatbuffers::{UnionWIPOffset, WIPOffset};
use crate::Schema_generated::org::apache::arrow::flatbuf::{Type, Int, IntArgs, FloatingPoint, FloatingPointArgs, Precision};

#[derive(Debug, Copy, Clone)]
pub struct OverflowError {}

pub trait ArrowPrimitive: Copy+Clone {
    fn arrow_type_type() -> Type;

    fn arrow_type<'a>(builder: &'a mut flatbuffers::FlatBufferBuilder) -> WIPOffset<UnionWIPOffset>;
}

macro_rules! impl_arrow_primitive_int {
    ($t:ty, $w:expr, $s:expr) => {
        impl ArrowPrimitive for $t {
            fn arrow_type_type() -> Type {
                Type::Int
            }

            fn arrow_type<'a>(builder: &'a mut flatbuffers::FlatBufferBuilder) -> WIPOffset<UnionWIPOffset> {
                Int::create(builder, &IntArgs{
                    bitWidth: $w,
                    is_signed: $s,
                }).as_union_value()
            }
        }
    }
}

macro_rules! impl_arrow_float {
    ($t:ty, $w:expr) => {
        impl ArrowPrimitive for $t {
            fn arrow_type_type() -> Type {
                Type::FloatingPoint
            }

            fn arrow_type<'a>(builder: &'a mut flatbuffers::FlatBufferBuilder) -> WIPOffset<UnionWIPOffset> {
                FloatingPoint::create(builder, &FloatingPointArgs{
                    precision: $w
                }).as_union_value()
            }
        }
    }
}

impl_arrow_primitive_int!(u8, 8, false);
impl_arrow_primitive_int!(i8, 8, true);
impl_arrow_primitive_int!(u16, 16, false);
impl_arrow_primitive_int!(i16, 16, true);
impl_arrow_primitive_int!(u32, 32, false);
impl_arrow_primitive_int!(i32, 32, true);
impl_arrow_primitive_int!(u64, 64, false);
impl_arrow_primitive_int!(i64, 64, true);
impl_arrow_float!(f32, Precision::SINGLE);
impl_arrow_float!(f64, Precision::DOUBLE);

pub struct BackedMutableScalarArray<'a, T>
where
T: Copy+Clone
{
    backing: &'a mut [u8],
    length: usize, // Logical length of array
    _type: PhantomData<T>
}

impl<'a, T> BackedMutableScalarArray<'a, T>
where
    T: Copy+Clone
{
    pub fn new_empty_from_u8(backing: &'a mut [u8]) -> Self {
        let align = core::mem::align_of::<T>();
        let is_aligned = backing.as_ptr().align_offset(align) == 0;

        assert!(is_aligned, "Slice is not aligned with the specified scalar type");

        // Trim the backing slice to have a size aligned with 8 bytes
        let new_len = (backing.len()/8)*8;

        BackedMutableScalarArray {
            backing: &mut backing[0..new_len],
            length: 0,
            _type: PhantomData
        }
    }

    pub fn new_empty(backing: &'a mut [T]) -> Self {
        let len = backing.len() * core::mem::size_of::<T>();
        let ptr = backing.as_mut_ptr() as *mut u8;

        let adjusted_backing : & mut[u8] = unsafe {
            &mut (*slice_from_raw_parts_mut(ptr, len))
        };

        // Trim the backing slice to have a size aligned with 8 bytes
        let new_len = (len/8)*8;

        BackedMutableScalarArray {
            backing: &mut adjusted_backing[0..new_len],
            length: 0,
            _type: PhantomData
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        // SAFETY
        //
        // - We are aligned and initialized for len elements of T
        // - We correspond to a single allocation
        // - We do not support modification whilst active immutable borrows
        unsafe { core::slice::from_raw_parts(self.backing.as_ptr() as _, self.length) }
    }

    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        // SAFETY
        //
        // - MutableBuffer is aligned and initialized for len elements of T
        // - MutableBuffer corresponds to a single allocation
        // - MutableBuffer does not support modification whilst active immutable borrows
        unsafe { core::slice::from_raw_parts_mut(self.backing.as_mut_ptr() as _, self.length) }
    }

    pub fn append(&mut self, value: T) -> Result<(), OverflowError> {
        // Check for overflow
        if (self.length+1) * core::mem::size_of::<T>() > self.backing.len() {
            return Err(OverflowError{})
        }
        // Write new element
        let ptr = self.backing.as_mut_ptr() as *mut T;
        unsafe {
            ptr.add(self.length).write(value);
        }
        self.length += 1;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn len_in_bytes(&self) -> usize {
        self.length * core::mem::size_of::<T>()
    }

    pub fn len_in_bytes_aligned(&self) -> usize {
        let raw_len = self.len_in_bytes();
        (raw_len/8 + (raw_len%8 != 0) as usize)*8
    }

    pub fn as_aligned_slice(&self) -> &[u8] {
        &self.backing[0..self.len_in_bytes_aligned()]
    }
}

impl <T> Index<usize> for BackedMutableScalarArray<'_, T>
where
    T: Copy+Clone
{
    type Output = T;
    fn index(&self, index: usize) -> &Self::Output {
        &self.as_slice()[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_test() {
        let mut backing = [0u8; 100];
        let mut array = BackedMutableScalarArray::new_empty_from_u8(&mut backing);
        assert_eq!(array.len(), 0);
        assert_eq!(array.as_slice().len(), 0);
        assert_eq!(array.as_slice_mut().len(), 0);
        array.append(1).unwrap();
        assert_eq!(array.len(), 1);
        assert_eq!(array.as_slice().len(), 1);
        assert_eq!(array.as_slice_mut().len(), 1);
        assert_eq!(array[0], 1);
        array.append(2).unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.as_slice().len(), 2);
        assert_eq!(array.as_slice_mut().len(), 2);
        assert_eq!(array[1], 2);
        array.append(3).unwrap();
        assert_eq!(array[2], 3);
    }
}