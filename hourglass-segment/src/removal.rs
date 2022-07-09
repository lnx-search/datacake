use std::io;
use std::path::Path;
use uuid::Uuid;

use glommio::GlommioError;
use crate::error::{Result, SegmentError};

#[inline]
pub async fn remove_segment(id: Uuid, base: &Path) -> Result<()> {
    match glommio::io::remove(base.join(id.to_string())).await {
        Ok(()) => Ok(()),
        Err(GlommioError::IoError(ref e)) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(GlommioError::IoError(e)) => Err(SegmentError::IoError(e)),
        Err(other) => Err(SegmentError::GlommioError(other.to_string())),
    }
}
