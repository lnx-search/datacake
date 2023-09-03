use std::fmt::{Debug, Formatter};
use std::mem;
use std::ops::Deref;

use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::{AlignedVec, Archive, Deserialize};

#[derive(Debug, thiserror::Error)]
#[error("View cannot be made for type with provided data.")]
/// The data provided is unable to be presented as the archived version
/// of the view type.
pub struct InvalidView;

#[repr(C)]
/// A block of data that can be accessed as if it is the archived value `T`.
///
/// This allows for safe, true zero-copy deserialization avoiding unnecessary
/// allocations if the situation does not require having an owned version of the value.
pub struct DataView<T>
where
    T: Archive,
    T::Archived: 'static,
{
    /// The view reference which lives as long as `data: D`.
    view: &'static rkyv::Archived<T>,
    /// The owned buffer itself.
    ///
    /// This must live as long as the view derived from it.
    data: AlignedVec,
}

impl<T> DataView<T>
where
    T: Archive,
    T::Archived: 'static,
{
    /// Creates a new view using a provided buffer.
    pub(crate) fn using(data: AlignedVec) -> Result<Self, InvalidView> {
        // SAFETY:
        //  This is safe as we own the data and keep it apart
        //  of the view itself.
        let extended_buf =
            unsafe { mem::transmute::<&[u8], &'static [u8]>(data.as_slice()) };

        if extended_buf.len() < 4 {
            return Err(InvalidView);
        }

        let end = extended_buf.len();
        let checksum_bytes = extended_buf[end - 4..]
            .try_into()
            .map_err(|_| InvalidView)?;
        let expected_checksum = u32::from_le_bytes(checksum_bytes);

        let data_bytes = &extended_buf[..end - 4];
        let actual_checksum = crc32fast::hash(data_bytes);

        if expected_checksum != actual_checksum {
            return Err(InvalidView);
        }

        let view = unsafe { rkyv::archived_root::<T>(data_bytes) };

        Ok(Self { data, view })
    }

    #[inline]
    /// Gets the bytes representation of the dataview.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    /// Consumes the bytes representation of the dataview.
    pub fn into_data(self) -> AlignedVec {
        self.data
    }
}

impl<T> DataView<T>
where
    T: Archive,
    T::Archived: Deserialize<T, SharedDeserializeMap> + 'static,
{
    #[inline]
    /// Deserializes the view into it's owned value T.
    pub fn to_owned(&self) -> Result<T, InvalidView> {
        self.view
            .deserialize(&mut SharedDeserializeMap::default())
            .map_err(|_| InvalidView)
    }
}

impl<T> Clone for DataView<T>
where
    T: Archive,
    T::Archived: Debug + 'static,
{
    fn clone(&self) -> Self {
        Self::using(self.data.clone()).expect("BUG: Valid data has become invalid?")
    }
}

impl<T> Debug for DataView<T>
where
    T: Archive,
    T::Archived: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.view.fmt(f)
    }
}

impl<T> Deref for DataView<T>
where
    T: Archive,
{
    type Target = T::Archived;

    fn deref(&self) -> &Self::Target {
        self.view
    }
}

impl<T> PartialEq for DataView<T>
where
    T: Archive,
    T::Archived: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.view == other.view
    }
}

impl<T> PartialEq<T> for DataView<T>
where
    T: Archive,
    T::Archived: PartialEq<T>,
{
    fn eq(&self, other: &T) -> bool {
        self.view == other
    }
}

#[cfg(test)]
mod tests {
    use rkyv::{Archive, Deserialize, Serialize};

    use super::*;

    #[repr(C)]
    #[derive(Serialize, Deserialize, Archive, PartialEq, Eq, Debug)]
    #[archive(compare(PartialEq), check_bytes)]
    #[archive_attr(derive(Debug, PartialEq, Eq))]
    struct Demo {
        a: String,
        b: u64,
    }

    #[test]
    fn test_view() {
        let demo = Demo {
            a: "Jello".to_string(),
            b: 133,
        };

        let bytes = crate::rkyv_tooling::to_view_bytes(&demo).unwrap();
        let view: DataView<Demo> = DataView::using(bytes).unwrap();
        assert!(view == demo, "Original and view must match.");
    }

    #[test]
    fn test_view_missing_checksum() {
        let demo = Demo {
            a: "Jello".to_string(),
            b: 133,
        };

        let bytes = rkyv::to_bytes::<_, 1024>(&demo).unwrap();
        DataView::<Demo>::using(bytes).expect_err("System should return invalid view.");
    }

    #[test]
    fn test_invalid_view() {
        let mut data = AlignedVec::new();
        data.extend_from_slice(b"Hello, world!");
        let res = DataView::<Demo>::using(data);
        assert!(res.is_err(), "View should be rejected");
    }

    #[test]
    fn test_deserialize() {
        let demo = Demo {
            a: "Jello".to_string(),
            b: 133,
        };

        let bytes = crate::rkyv_tooling::to_view_bytes(&demo).unwrap();
        let view: DataView<Demo> = DataView::using(bytes).unwrap();
        assert!(view == demo, "Original and view must match.");

        let value = view.to_owned().unwrap();
        assert_eq!(value, demo, "Deserialized and original value should match.")
    }
}
