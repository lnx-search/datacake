use bytes::Buf;
use hyper::body::HttpBody;
use hyper::Body;
use rkyv::AlignedVec;

pub async fn to_aligned(
    mut body: Body,
) -> Result<AlignedVec, <Body as HttpBody>::Error> {
    // If there's only 1 chunk, we can just return Buf::to_bytes()
    let first = if let Some(buf) = body.data().await {
        buf?
    } else {
        return Ok(AlignedVec::new());
    };

    let second = if let Some(buf) = body.data().await {
        buf?
    } else {
        let mut vec = AlignedVec::with_capacity(first.len());
        vec.extend_from_slice(&first);
        return Ok(vec);
    };

    // With more than 1 buf, we gotta flatten into a Vec first.
    let cap = first.remaining() + second.remaining() + body.size_hint().lower() as usize;
    let mut vec = AlignedVec::with_capacity(cap);
    vec.extend_from_slice(&first);
    vec.extend_from_slice(&second);

    while let Some(buf) = body.data().await {
        vec.extend_from_slice(&buf?);
    }

    Ok(vec)
}
