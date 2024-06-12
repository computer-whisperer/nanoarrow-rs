use log::debug;

pub struct HPackTable {

}

impl HPackTable {
    pub fn new() -> HPackTable {
        HPackTable {

        }
    }

    pub fn static_table(index: u32) -> (&'static str, Option<&'static str>) {
        match index {
            1 => (":authority", None),
            2 => (":method", Some("GET")),
            3 => (":method", Some("POST")),
            4 => (":path", Some("/")),
            5 => (":path", Some("/index.html")),
            6 => (":scheme", Some("http")),
            7 => (":scheme", Some("https")),
            8 => (":status", Some("200")),
            9 => (":status", Some("204")),
            10 => (":status", Some("206")),
            11 => (":status", Some("304")),
            12 => (":status", Some("400")),
            13 => (":status", Some("404")),
            14 => (":status", Some("500")),
            15 => ("accept-charset", None),
            16 => ("accept-encoding", Some("gzip, deflate")),
            17 => ("accept-language", None),
            18 => ("accept-ranges", None),
            19 => ("accept", None),
            20 => ("access-control-allow-origin", None),
            21 => ("age", None),
            22 => ("allow", None),
            23 => ("authorization", None),
            24 => ("cache-control", None),
            25 => ("content-disposition", None),
            26 => ("content-encoding", None),
            27 => ("content-language", None),
            28 => ("content-length", None),
            29 => ("content-location", None),
            30 => ("content-range", None),
            _ => {panic!("Invalid index")}
        }
    }

    pub fn get_name_and_value(&self, index: usize) -> (&str, &str) {
        let (a, b) = Self::static_table(index as u32);
        (a, b.unwrap())
    }
}

pub struct Encoder {
    hpack_table: HPackTable
}

impl Encoder {

    pub fn new() -> Self {
        Self {
            hpack_table: HPackTable::new()
        }
    }

    pub fn encode_number(dest: &mut [u8], prefix: u8, prefix_size: u8, number: usize) -> usize {
        let first_num_mask = ((1<<prefix_size) - 1) as usize;
        let prefix_mask =  !(first_num_mask as u8);
        let mut bytes_written = 0;
        if number < first_num_mask {
            dest[0] = (prefix & prefix_mask) | ((number as u8) & (first_num_mask as u8));
            bytes_written += 1;
        }
        else {
            dest[bytes_written] = (prefix & prefix_mask) | (first_num_mask as u8);
            bytes_written += 1;
            let mut number = number - first_num_mask;
            while number > 128 {
                dest[bytes_written] = ((number as u8)%0x80) | 0x80;
                bytes_written += 1;
                number = number / 128
            }
            dest[bytes_written] = number as u8;
            bytes_written += 1;
        }

        bytes_written
    }

    pub fn encode_string_literal(dest: &mut [u8], value: &str) -> usize {
        let mut bytes_written = 0;
        bytes_written += Self::encode_number(&mut dest[bytes_written..], 0x00, 7, value.len());
        dest[bytes_written .. bytes_written+value.len()].copy_from_slice(value.as_bytes());
        bytes_written += value.len();
        bytes_written
    }

    pub fn encode_literal_header(dest: &mut [u8], name: &str, value: &str) -> usize {
        let mut bytes_written = 0;
        dest[0] = 0x00;
        bytes_written += 1;
        bytes_written += Self::encode_string_literal(&mut dest[bytes_written..], name);
        bytes_written += Self::encode_string_literal(&mut dest[bytes_written..], value);
        bytes_written
    }

    pub fn encode_header_list<'a, const L: usize>(&mut self, plaintext_header_list: PlaintextHeaderList<L>, buffer: &'a mut [u8]) -> &'a [u8] {
        let mut bytes_written = 0;
        for header in &plaintext_header_list.raw_headers[..plaintext_header_list.num_headers] {
            bytes_written += Self::encode_literal_header(&mut buffer[bytes_written..], header.0, header.1);
        }
        &buffer[..bytes_written]
    }
}


pub struct Decoder {
    hpack_table: HPackTable
}

impl Decoder {

    pub fn new() -> Self {
        Self {
            hpack_table: HPackTable::new()
        }
    }

    pub fn decode_number(src: &[u8], prefix_size: u8) -> (usize, usize) {
        let first_num_mask = ((1<<prefix_size) - 1) as usize;
        let mut number = src[0] as usize & first_num_mask;
        let mut bytes_read = 1;
        if number == first_num_mask {
            let mut m = 0;
            while src[bytes_read] & 0x80 == 0x80 {
                number += ((src[bytes_read] & 0x7f) as usize) * (1 << m);
                bytes_read += 1;
                m += 7;
            }
            number += ((src[bytes_read] & 0x7f) as usize) * (1 << m);
            bytes_read += 1;
        }
        (number, bytes_read)
    }

    pub fn decode_string(src: &[u8]) -> (&str, usize) {
        if (src[0] & 0x80) == 0x80 {
            // Huffman coding not implemented
            panic!("Huffman coding not implemented")
        }
        else {
            let (length, bytes_read) = Self::decode_number(src, 7);
            let string = core::str::from_utf8(&src[bytes_read..bytes_read+length]).unwrap();
            (string, bytes_read+length)
        }
    }

    pub fn decode_header_list<'a, const L: usize>(&mut self, encoded_header_list: &'_ [u8], buffer: &'a mut [u8]) -> PlaintextHeaderList<'a, L> {
        let mut bytes_written = 0;
        let mut bytes_read = 0;
        let mut header_slice_list: [((usize, usize),(usize, usize)); L] = [((0, 0), (0, 0)); L];
        let mut num_headers_found = 0;
        while encoded_header_list.len() > bytes_read {
            if encoded_header_list[bytes_read]&0x80 == 0x80 {
                // Indexed field
                let (index, r) = Self::decode_number(&encoded_header_list[bytes_read..], 7);
                bytes_read += r;
                let (name, value) = self.hpack_table.get_name_and_value(index);
                let name_range = {
                    let s = name;
                    let range = (bytes_written, bytes_written+s.len());
                    buffer[range.0..range.1].copy_from_slice(s.as_bytes());
                    bytes_written += s.len();
                    range
                };
                let value_range = {
                    let s = value;
                    let range = (bytes_written, bytes_written+s.len());
                    buffer[range.0..range.1].copy_from_slice(s.as_bytes());
                    bytes_written += s.len();
                    range
                };
                header_slice_list[num_headers_found] = (name_range, value_range);
                num_headers_found += 1;
            }
            else if encoded_header_list[bytes_read]&0x40 == 0x40 {
                // Literal Header Field with Incremental Indexing
                debug!("Literal Header Field with Incremental Indexing not implemented");
                break;
            }
            else if encoded_header_list[bytes_read]&0x20 == 0x20 {
                // Dynamic table size update
                debug!("Dynamic table size update not implemented");
                break;
            }
            else {
                // Literal Header Field never indexed or without indexing
                if encoded_header_list[bytes_read]&0x0F != 0x00
                {
                    panic!("Indexing not implemented")
                }
                else
                {
                    bytes_read += 1;
                    let name_range = {
                        let (s, n) = Self::decode_string(&encoded_header_list[bytes_read..]);
                        bytes_read += n;
                        let range = (bytes_written, bytes_written+s.len());
                        buffer[range.0..range.1].copy_from_slice(s.as_bytes());
                        bytes_written += s.len();
                        range
                    };
                    let value_range = {
                        let (s, n) = Self::decode_string(&encoded_header_list[bytes_read..]);
                        bytes_read += n;
                        let range = (bytes_written, bytes_written+s.len());
                        buffer[range.0..range.1].copy_from_slice(s.as_bytes());
                        bytes_written += s.len();
                        range
                    };
                    header_slice_list[num_headers_found] = (name_range, value_range);
                    num_headers_found += 1;
                }
            }
        };

        // Remove mutability
        let buffer: &'a [u8] = buffer;
        let mut plaintext_header_list = PlaintextHeaderList::<'a, L>::new();
        for i in 0..num_headers_found {
            let ((a, b), (c, d)) = header_slice_list[i];
            plaintext_header_list.raw_headers[plaintext_header_list.num_headers] =
                (core::str::from_utf8(&buffer[a..b]).unwrap(), core::str::from_utf8(&buffer[c..d]).unwrap());
            plaintext_header_list.num_headers += 1;
        }
        plaintext_header_list
    }
}


#[derive(Clone)]
pub struct PlaintextHeaderList<'a, const L: usize> {
    raw_headers: [(&'a str, &'a str); L],
    num_headers: usize
}

impl<'a, const L: usize> PlaintextHeaderList<'a, L> {
    pub fn new() -> Self {
        Self {
            raw_headers: [("", ""); L],
            num_headers: 0
        }
    }

    pub fn add_header(&mut self, name: &'a str, value: &'a str) {
        self.raw_headers[self.num_headers] = (name, value);
        self.num_headers += 1;
    }
}

impl<'a, const L: usize> core::fmt::Debug for PlaintextHeaderList<'a, L> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for (name, value) in &self.raw_headers[0..self.num_headers]
        {
            write!(f, "({} = {}),", name, value)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_encode_decode_number(number: usize, prefix_size: u8) {
        let mut buffer = [0u8; 10];
        let bytes_written = Encoder::encode_number(&mut buffer, 0x00, prefix_size, number);
        let (decoded_number, bytes_read) = Decoder::decode_number(&buffer[0..bytes_written], prefix_size);
        assert_eq!(number, decoded_number);
        assert_eq!(bytes_written, bytes_read);
    }

    #[test]
    fn basic_tests() {
        let numbers = [0, 5, 10, 50, 200, 1240];
        let prefixes = [1, 2, 3, 4, 7];
        for n in numbers {
            for p in prefixes {
                test_encode_decode_number(n, p);
            }
        }
    }
}


