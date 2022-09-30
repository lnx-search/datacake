use std::ops::{Deref, DerefMut};

use bytes::Bytes;
use datacake_crdt::{BadState, OrSWotSet};

#[derive(Default)]
pub struct LocalShard {
    state: OrSWotSet,
    compressed_view: Bytes,
    decompressed_len: usize,
    is_compressed: bool,
}

impl Deref for LocalShard {
    type Target = OrSWotSet;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for LocalShard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl LocalShard {
    pub async fn transportable_buffer(&mut self) -> Result<(Bytes, u64), BadState> {
        let buffer = self.try_compress_data().await?;
        Ok((buffer, self.decompressed_len as u64))
    }

    pub async fn try_compress_data(&mut self) -> Result<Bytes, BadState> {
        if self.is_compressed {
            return Ok(self.compressed_view.clone());
        }

        let (decompressed_len, compressed) =
            crate::shared::compress_set(&self.state).await?;
        let compressed = Bytes::from(compressed);

        self.state = OrSWotSet::default();
        self.compressed_view = compressed;
        self.decompressed_len = decompressed_len;
        self.is_compressed = true;

        Ok(self.compressed_view.clone())
    }

    pub async fn try_decompress_data(&mut self) -> Result<(), BadState> {
        if !self.is_compressed {
            return Ok(());
        }

        let state =
            crate::shared::decompress_set(&self.compressed_view, self.decompressed_len)
                .await?;

        self.state = state;
        self.compressed_view = Bytes::new();
        self.is_compressed = false;
        self.decompressed_len = 0;

        Ok(())
    }
}
