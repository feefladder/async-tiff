use crate::{
    bytecast::*,
    error::{AsyncTiffError, AsyncTiffResult},
    predictor::PredictorInfo,
    tiff::{tags::SampleFormat, TiffError, TiffUnsupportedError},
};

/// Result of a decoding process
#[derive(Debug)]
#[non_exhaustive]
pub enum DecodingResult {
    /// A vector of unsigned bytes
    U8(Vec<u8>),
    /// A vector of unsigned words
    U16(Vec<u16>),
    /// A vector of 32 bit unsigned ints
    U32(Vec<u32>),
    /// A vector of 64 bit unsigned ints
    U64(Vec<u64>),
    /// A vector of 32 bit IEEE floats
    F32(Vec<f32>),
    /// A vector of 64 bit IEEE floats
    F64(Vec<f64>),
    /// A vector of 8 bit signed ints
    I8(Vec<i8>),
    /// A vector of 16 bit signed ints
    I16(Vec<i16>),
    /// A vector of 32 bit signed ints
    I32(Vec<i32>),
    /// A vector of 64 bit signed ints
    I64(Vec<i64>),
}

impl DecodingResult {
    /// use this result as a `&mut[u8]` buffer
    pub fn as_mut_u8_buf(&mut self) -> &mut [u8] {
        match self {
            DecodingResult::U8(v) => v,
            DecodingResult::U16(v) => u16_as_ne_mut_bytes(v),
            DecodingResult::U32(v) => u32_as_ne_mut_bytes(v),
            DecodingResult::U64(v) => u64_as_ne_mut_bytes(v),
            DecodingResult::I8(v) => i8_as_ne_mut_bytes(v),
            DecodingResult::I16(v) => i16_as_ne_mut_bytes(v),
            DecodingResult::I32(v) => i32_as_ne_mut_bytes(v),
            DecodingResult::I64(v) => i64_as_ne_mut_bytes(v),
            DecodingResult::F32(v) => f32_as_ne_mut_bytes(v),
            DecodingResult::F64(v) => f64_as_ne_mut_bytes(v),
        }
    }

    /// use this result as a `&[u8]` buffer
    pub fn as_u8_buf(&self) -> &[u8] {
        match self {
            DecodingResult::U8(v) => v,
            DecodingResult::U16(v) => u16_as_ne_bytes(v),
            DecodingResult::U32(v) => u32_as_ne_bytes(v),
            DecodingResult::U64(v) => u64_as_ne_bytes(v),
            DecodingResult::I8(v) => i8_as_ne_bytes(v),
            DecodingResult::I16(v) => i16_as_ne_bytes(v),
            DecodingResult::I32(v) => i32_as_ne_bytes(v),
            DecodingResult::I64(v) => i64_as_ne_bytes(v),
            DecodingResult::F32(v) => f32_as_ne_bytes(v),
            DecodingResult::F64(v) => f64_as_ne_bytes(v),
        }
    }

    /// create a properly sized `Self` from PredictorInfo struct for a single chunk (tile/strip)
    // similar to image-tiff's Decoder::result_buffer
    pub fn from_predictor_info(
        info: PredictorInfo,
        chunk_x: usize,
        chunk_y: usize,
    ) -> AsyncTiffResult<Self> {
        // this part is outside of result_buffer

        // since the calculations are in pixels rather than bytes (predictors
        // use bytes), we do them here, rather than adding even more functions
        // to PredictorInfo
        let width = info.chunk_width_pixels(chunk_x as _)?;
        let height = info.output_rows(chunk_y as _)?;

        let buffer_size = match (width as usize)
            .checked_mul(height)
            .and_then(|x| x.checked_mul(info.samples_per_pixel as _))
        {
            Some(s) => s,
            None => return Err(AsyncTiffError::InternalTIFFError(TiffError::IntSizeError)),
        };

        let max_sample_bits = info.bits_per_sample();

        match info.sample_format {
            SampleFormat::Uint => match max_sample_bits {
                n if n <= 8 => Ok(DecodingResult::U8(vec![0u8; buffer_size])),
                n if n <= 16 => Ok(DecodingResult::U16(vec![0u16; buffer_size])),
                n if n <= 32 => Ok(DecodingResult::U32(vec![0u32; buffer_size])),
                n if n <= 64 => Ok(DecodingResult::U64(vec![0u64; buffer_size])),
                n => Err(AsyncTiffError::InternalTIFFError(
                    TiffError::UnsupportedError(TiffUnsupportedError::UnsupportedBitsPerChannel(n)),
                )),
            },
            SampleFormat::Int => match max_sample_bits {
                n if n <= 8 => Ok(DecodingResult::I8(vec![0i8; buffer_size])),
                n if n <= 16 => Ok(DecodingResult::I16(vec![0i16; buffer_size])),
                n if n <= 32 => Ok(DecodingResult::I32(vec![0i32; buffer_size])),
                n if n <= 64 => Ok(DecodingResult::I64(vec![0i64; buffer_size])),
                n => Err(AsyncTiffError::InternalTIFFError(
                    TiffError::UnsupportedError(TiffUnsupportedError::UnsupportedBitsPerChannel(n)),
                )),
            },
            SampleFormat::IEEEFP => match max_sample_bits {
                32 => Ok(DecodingResult::F32(vec![0f32; buffer_size])),
                64 => Ok(DecodingResult::F64(vec![0f64; buffer_size])),
                n => Err(AsyncTiffError::InternalTIFFError(
                    TiffError::UnsupportedError(TiffUnsupportedError::UnsupportedBitsPerChannel(n)),
                )),
            },
            format => Err(AsyncTiffError::InternalTIFFError(
                TiffUnsupportedError::UnsupportedSampleFormat(vec![format]).into(),
            )),
        }
    }
}
