use std::fmt::{Debug, Formatter};
use std::mem;
use std::ops::Deref;
use rkyv::{AlignedVec, Archive};
use bytecheck::{CheckBytes};
use rkyv::validation::validators::DefaultValidator;


#[derive(Debug, thiserror::Error)]
#[error("View cannot be made for type with provided data.")]
/// The data provided is unable to be presented as the archived version
/// of the view type.
pub struct InvalidView;


/// A block of data that can be accessed as if it is the archived value `T`.
///
/// This allows for safe, true zero-copy deserialization avoiding unnecessary
/// allocations if the situation does not require having an owned version of the value.
pub struct DataView<T, D = AlignedVec>
where
    T: Archive,
    T::Archived: 'static,
    D: Deref<Target = [u8]> + Send + Sync,
{
    /// The owned buffer itself.
    ///
    /// This must live as long as the view derived from it.
    data: D,

    /// The view reference which lives as long as `data: D`.
    view: &'static rkyv::Archived<T>,
}

impl<T, D> DataView<T, D>
where
    T: Archive,
    T::Archived: CheckBytes<DefaultValidator<'static>> + 'static,
    D: Deref<Target = [u8]> + Send + Sync,
{
    /// Creates a new view using a provided buffer.
    pub(crate) fn using(data: D) -> Result<Self, InvalidView> {
        // SAFETY:
        //  This is safe as we own the data and keep it apart
        //  of the view itself.
        let extended_buf = unsafe {
            mem::transmute::<&[u8], &'static [u8]>(&data)
        };

        let view = rkyv::check_archived_root::<'_, T>(extended_buf)
            .map_err(|_| InvalidView)?;

        Ok(Self {
            data,
            view,
        })
    }
}

impl<T, D> Clone for DataView<T, D>
where
    T: Archive,
    T::Archived: CheckBytes<DefaultValidator<'static>> + Debug,
    D: Deref<Target = [u8]> + Send + Sync + Clone,
{
    fn clone(&self) -> Self {
        Self::using(self.data.clone())
            .expect("BUG: Valid data has become invalid?")
    }
}

impl<T, D> Debug for DataView<T, D>
where
    T: Archive,
    T::Archived: CheckBytes<DefaultValidator<'static>> + Debug,
    D: Deref<Target = [u8]> + Send + Sync,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.view.fmt(f)
    }
}

impl<T, D> Deref for DataView<T, D>
where
    T: Archive,
    T::Archived: CheckBytes<DefaultValidator<'static>>,
    D: Deref<Target = [u8]> + Send + Sync,
{
    type Target = T::Archived;

    fn deref(&self) -> &Self::Target {
        self.view
    }
}

impl<T, D> PartialEq for DataView<T, D>
where
    T: Archive,
    T::Archived: CheckBytes<DefaultValidator<'static>> + PartialEq,
    D: Deref<Target = [u8]> + Send + Sync,
{
    fn eq(&self, other: &Self) -> bool {
        self.view == other.view
    }
}

impl<T, D> PartialEq<T> for DataView<T, D>
where
    T: Archive,
    T::Archived: CheckBytes<DefaultValidator<'static>> + PartialEq<T>,
    D: Deref<Target = [u8]> + Send + Sync,
{
    fn eq(&self, other: &T) -> bool {
        self.view == other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytecheck::CheckBytes;
    use rkyv::{Serialize, Deserialize, Archive};

    #[repr(C)]
    #[derive(Serialize, Deserialize, Archive, PartialEq, Eq, Debug)]
    #[archive(compare(PartialEq))]
    #[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
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

        let bytes = rkyv::to_bytes::<_, 1024>(&demo).unwrap();
        let view: DataView<Demo, _> = DataView::using(bytes).unwrap();
        assert!(view == demo , "Original and view must match.");
    }
}

