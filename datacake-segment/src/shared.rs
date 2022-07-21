use std::path::{Path, PathBuf};

use uuid::Uuid;

#[inline]
pub(crate) fn segment_path(id: Uuid, base: &Path) -> PathBuf {
    base.join(id.to_string()).with_extension("seg")
}
