use datacake_crdt::{BadState, OrSWotSet};
use lz4_flex::block::DecompressError;

/// Compresses a continuous buffer of documents.
pub async fn compress_docs(docs: Vec<u8>) -> Vec<u8> {
    tokio::task::spawn_blocking(move || lz4_flex::compress(&docs))
        .await
        .expect("spawn background thread")
}

/// Decompresses the given document buffer with a known uncompressed length.
pub async fn decompress_docs(
    docs: &[u8],
    uncompressed_len: usize,
) -> Result<Vec<u8>, DecompressError> {
    // This is to save potentially re-allocating many GB of data just for the sake of compression
    // the reason why this isn't requiring an owned reference is because potentially optimisations
    // can be taken outside of this process to avoid unnecessary memory usage.
    //
    // SAFETY:
    //   This is safe to do because we know that this 'false buffer' will always exist longer
    //   than the thread reading the buffer.
    let false_buffer = unsafe { std::mem::transmute::<_, &'static [u8]>(docs) };

    tokio::task::spawn_blocking(move || {
        lz4_flex::decompress(false_buffer, uncompressed_len)
    })
    .await
    .expect("spawn background thread")
}

/// Serializes and compresses a given [OrSwotSet]
pub async fn compress_set(set: &OrSWotSet) -> Result<(usize, Vec<u8>), BadState> {
    let buffer = set.as_bytes()?;
    let uncompressed_len = buffer.len();

    let compressed = tokio::task::spawn_blocking(move || lz4_flex::compress(&buffer))
        .await
        .expect("spawn background thread");

    Ok((uncompressed_len, compressed))
}

/// Decompresses and deserializes a given [OrSwotSet] buffer.
pub async fn decompress_set(
    buffer: &[u8],
    uncompressed_len: usize,
) -> Result<OrSWotSet, BadState> {
    // This is to save potentially re-allocating many GB of data just for the sake of compression
    // the reason why this isn't requiring an owned reference is because potentially optimisations
    // can be taken outside of this process to avoid unnecessary memory usage.
    //
    // SAFETY:
    //   This is safe to do because we know that this 'false buffer' will always exist longer
    //   than the thread reading the buffer.
    let false_buffer = unsafe { std::mem::transmute::<_, &'static [u8]>(buffer) };

    tokio::task::spawn_blocking(move || {
        let decompressed = lz4_flex::decompress(false_buffer, uncompressed_len)
            .map_err(|_| BadState)?;

        OrSWotSet::from_bytes(&decompressed)
    })
    .await
    .expect("spawn background thread")
}
