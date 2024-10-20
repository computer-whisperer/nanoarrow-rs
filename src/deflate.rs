#![no_std]

/// Structure representing the compression context
pub struct UzlibComp<'a, const HASH_BITS: usize, T>
where
    T: Copy + Default + core::cmp::Ord,
    [(); 1 << HASH_BITS]:
{
    outbuf: &'a mut [u8],            // Mutable reference to the output buffer
    outlen: usize,                   // Current length of output data
    outbits: u32,                    // Pending output bits
    noutbits: u8,                    // Number of pending bits
    hash_table: [T; 1 << HASH_BITS], // Hash table for compression
}

impl<'a, const HASH_BITS: usize, T> UzlibComp<'a, HASH_BITS, T>
where
    T: Copy + Default + core::cmp::Ord + From<u8> + Into<usize>,
    [(); 1 << HASH_BITS]:
{
    /// Creates a new compression context
    pub fn new(outbuf: &'a mut [u8]) -> Self {
        let max_value = Self::max_value();
        Self {
            outbuf,
            outlen: 0,
            outbits: 0,
            noutbits: 0,
            hash_table: [max_value; 1 << HASH_BITS], // Initialize with maximum value
        }
    }

    fn max_value() -> T {
        match core::mem::size_of::<T>() {
            1 => T::from(u8::MAX),
            2 => T::from(u16::MAX as u8),
            _ => T::from(u32::MAX as u8), // Default to u32::MAX as the fallback
        }
    }


    /// Function to write bits to the output buffer
    pub fn outbits(&mut self, bits: u32, nbits: u8) {
        assert!(self.noutbits as usize + nbits as usize <= 32, "Exceeds bit buffer size");
        self.outbits |= bits << self.noutbits;
        self.noutbits += nbits;

        while self.noutbits >= 8 {
            if self.outlen >= self.outbuf.len() {
                // Output buffer overflow handling
                return;
            }

            self.outbuf[self.outlen] = (self.outbits & 0xFF) as u8;
            self.outlen += 1;
            self.outbits >>= 8;
            self.noutbits -= 8;
        }
    }

    /// Function to finalize and flush the remaining bits to the output buffer
    pub fn finalize(&mut self) -> usize {
        while self.noutbits > 0 {
            if self.outlen >= self.outbuf.len() {
                // Output buffer overflow handling
                return self.outlen;
            }

            self.outbuf[self.outlen] = (self.outbits & 0xFF) as u8;
            self.outlen += 1;
            self.outbits >>= 8;
            self.noutbits -= 8;
        }

        self.outlen
    }

    /// Function to reset the hash table
    pub fn reset_hash_table(&mut self) {
        self.hash_table.fill(Self::max_value());
    }

    /// Function to insert a new substring into the hash table
    pub fn insert_hash(&mut self, position: usize, input: &[u8]) -> usize {
        let hash = self.hash(input, position);
        let previous_position = self.hash_table[hash].into();
        self.hash_table[hash] = T::from(position as u8);
        previous_position
    }

    /// Compute a hash value for a substring starting at `position`
    fn hash(&self, input: &[u8], position: usize) -> usize {
        let mut hash: usize = 0;
        for i in 0..3 {
            hash = (hash << 5) ^ (input[position + i] as usize);
        }
        hash & ((1 << HASH_BITS) - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reset_hash_table() {
        let mut buffer = [0u8; 256];
        let mut uzlib = UzlibComp::<8, u8>::new(&mut buffer);
        uzlib.reset_hash_table();
        assert!(uzlib.hash_table.iter().all(|&v| v == u8::MAX));
    }

    #[test]
    fn test_insert_hash() {
        let mut buffer = [0u8; 256];
        let mut uzlib = UzlibComp::<8, u8>::new(&mut buffer);
        let input = b"abcdefgabcdefg";
        uzlib.reset_hash_table();
        let prev_pos = uzlib.insert_hash(0, input);
        assert_eq!(prev_pos, u8::MAX as usize);
        let new_pos = uzlib.insert_hash(1, input);
        assert_eq!(new_pos, 0);
    }

    #[test]
    fn test_hash() {
        let mut buffer = [0u8; 256];
        let uzlib = UzlibComp::<8, u8>::new(&mut buffer);
        let input = b"abcdefg";
        let hash_value = uzlib.hash(input, 0);
        assert!(hash_value < (1 << 8));
    }
}
