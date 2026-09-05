use crate::error::{HarliteError, Result};

pub(crate) struct DecodedBody {
    pub bytes: Vec<u8>,
    pub encoded: bool,
}

/// HAR may retain Content-Encoding after the browser has decoded the body.
/// Recognize supported wire representations while retaining already-decoded data.
#[cfg_attr(not(feature = "compression"), allow(unused_mut))]
pub(crate) fn decode_captured(
    bytes: &[u8],
    encoding: Option<&str>,
    limit: usize,
) -> Result<DecodedBody> {
    let encodings: Vec<_> = encoding
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty() && s != "identity")
        .collect();
    let mut current = bytes.to_vec();
    let mut encoded = false;
    for encoding in encodings.iter().rev() {
        if matches!(encoding.as_str(), "gzip" | "x-gzip") && !current.starts_with(&[0x1f, 0x8b]) {
            if encoded {
                return Err(HarliteError::InvalidHar(
                    "Inconsistent body encoding chain".into(),
                ));
            }
            return Ok(DecodedBody {
                bytes: current,
                encoded: false,
            });
        }
        #[cfg(not(feature = "compression"))]
        {
            let _ = limit;
            return Err(HarliteError::InvalidArgs(format!(
                "Inspecting {encoding} bodies requires the compression feature"
            )));
        }
        #[cfg(feature = "compression")]
        {
            current = match encoding.as_str() {
                "gzip" | "x-gzip" => {
                    read_limited(flate2::read::MultiGzDecoder::new(current.as_slice()), limit)?
                }
                "br" => {
                    // A one-byte input buffer lets us reject trailing bytes, rather
                    // than mistaking an arbitrary decoded prefix for a Brotli stream.
                    let mut decoder = brotli::Decompressor::new(current.as_slice(), 1);
                    match read_limited(&mut decoder, limit) {
                        Ok(decoded) if decoder.into_inner().is_empty() => decoded,
                        Ok(_) | Err(HarliteError::Io(_)) if !encoded => {
                            return Ok(DecodedBody {
                                bytes: current,
                                encoded: false,
                            })
                        }
                        Err(error) => return Err(error),
                        _ => {
                            return Err(HarliteError::InvalidHar(
                                "Inconsistent Brotli body encoding".into(),
                            ))
                        }
                    }
                }
                _ => {
                    return Err(HarliteError::InvalidArgs(format!(
                        "Unsupported body Content-Encoding: {encoding}"
                    )))
                }
            };
            encoded = true;
        }
    }
    Ok(DecodedBody {
        bytes: current,
        encoded,
    })
}

#[cfg(feature = "compression")]
fn read_limited(reader: impl std::io::Read, limit: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    reader
        .take((limit as u64).saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(HarliteError::InvalidArgs(format!(
            "Decompressed body exceeds {limit} bytes; cannot inspect safely"
        )));
    }
    Ok(bytes)
}

#[cfg(all(test, feature = "compression"))]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn decompression_is_bounded_and_corrupt_gzip_is_an_error() {
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gzip.write_all(b"longer than the limit").unwrap();
        let bytes = gzip.finish().unwrap();
        assert!(decode_captured(&bytes, Some("gzip"), 4).is_err());
        assert!(decode_captured(&[0x1f, 0x8b, 0], Some("gzip"), 1024).is_err());
        let decoded = decode_captured(b"already decoded", Some("gzip"), 1024).unwrap();
        assert!(!decoded.encoded);
        assert_eq!(decoded.bytes, b"already decoded");
    }
}
