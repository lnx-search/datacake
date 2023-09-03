use rkyv::ser::serializers::{
    AlignedSerializer,
    CompositeSerializer,
    SharedSerializeMap,
};
use rkyv::ser::Serializer;
use rkyv::{AlignedVec, Fallible, Serialize};

mod scratch;
mod view;

use self::scratch::LazyScratch;
pub use self::view::{DataView, InvalidView};

pub(crate) type DatacakeSerializer =
    CompositeSerializer<AlignedSerializer<AlignedVec>, LazyScratch, SharedSerializeMap>;

#[inline]
/// Produces an aligned buffer of the serialized data with a CRC32 checksum attached
/// to the last 4 bytes of the buffer.
pub fn to_view_bytes<T>(
    value: &T,
) -> Result<AlignedVec, <DatacakeSerializer as Fallible>::Error>
where
    T: Serialize<DatacakeSerializer>,
{
    let mut serializer = DatacakeSerializer::new(
        AlignedSerializer::new(AlignedVec::with_capacity(512)),
        LazyScratch::default(),
        SharedSerializeMap::new(),
    );

    serializer.serialize_value(value)?;

    let serializer = serializer.into_serializer();
    let mut buffer = serializer.into_inner();

    let checksum = crc32fast::hash(&buffer);
    buffer.extend_from_slice(&checksum.to_le_bytes());

    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rkyv::{Archive, Serialize};

    use super::*;

    #[derive(Archive, Serialize)]
    #[archive(check_bytes)]
    struct FixedSize {
        a: u32,
        b: f64,
        c: i64,
        buf: [u8; 12],
    }

    #[derive(Archive, Serialize)]
    #[archive(check_bytes)]
    struct AllocatedSize {
        a: u32,
        b: f64,
        c: HashMap<String, u64>,
        buf: Vec<u8>,
    }

    #[test]
    fn test_static_size_serialize() {
        let val = FixedSize {
            a: 123,
            b: 1.23,
            c: -123,
            buf: Default::default(),
        };

        let buffer = to_view_bytes(&val).expect("Serialize struct");

        let end = buffer.len();

        rkyv::check_archived_root::<FixedSize>(&buffer[..end - 4])
            .expect("Get archived value");

        let checksum_bytes = buffer[end - 4..].try_into().unwrap();
        let expected_checksum = u32::from_le_bytes(checksum_bytes);
        let actual_checksum = crc32fast::hash(&buffer[..end - 4]);

        assert_eq!(expected_checksum, actual_checksum, "Checksums should match");
    }

    #[test]
    fn test_heap_size_serialize() {
        let val = AllocatedSize {
            a: 123,
            b: 1.23,
            c: {
                let mut map = HashMap::new();
                map.insert("hello".to_string(), 3);
                map
            },
            buf: vec![4; 10],
        };

        let buffer = to_view_bytes(&val).expect("Serialize struct");

        let end = buffer.len();

        rkyv::check_archived_root::<FixedSize>(&buffer[..end - 4])
            .expect("Get archived value");

        let checksum_bytes = buffer[end - 4..].try_into().unwrap();
        let expected_checksum = u32::from_le_bytes(checksum_bytes);
        let actual_checksum = crc32fast::hash(&buffer[..end - 4]);

        assert_eq!(expected_checksum, actual_checksum, "Checksums should match");
    }
}
