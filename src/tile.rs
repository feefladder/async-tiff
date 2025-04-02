use bytes::Bytes;

use crate::decoder::DecoderRegistry;
use crate::error::AsyncTiffResult;
use crate::predictor::RevPredictorRegistry;
use crate::reader::Endianness;
use crate::tiff::tags::{
    CompressionMethod, PhotometricInterpretation, PlanarConfiguration, Predictor, SampleFormat,
};
use crate::tiff::{TiffError, TiffUnsupportedError};

/// All info that may be used by a predictor
///
/// Most of this is used by the floating point predictor
/// since that intermixes padding into the decompressed output
///
/// Also provides convenience functions
///
#[derive(Debug, Clone, Copy)]
pub struct PredictorInfo<'a> {
    /// endianness
    pub endianness: Endianness,
    /// width of the image in pixels
    pub image_width: u32,
    /// height of the image in pixels
    pub image_height: u32,
    /// chunk width in pixels
    ///
    /// If this is a stripped tiff, `chunk_width=image_width`
    pub chunk_width: u32,
    /// chunk height in pixels
    pub chunk_height: u32,
    /// bits per sample, as an array
    ///
    /// Can also be a single value, in which case it applies to all samples
    pub bits_per_sample: &'a [u16], // maybe say that we only support a single bits_per_sample?
    /// number of samples per pixel
    pub samples_per_pixel: u16,
    /// sample format for each sample
    ///
    /// There is no decoding implementation in this crate (or libtiff) for mixed sample formats
    pub sample_format: &'a [SampleFormat], // and a single sample_format?
    /// planar configuration
    ///
    /// determines the bits per pixel
    pub planar_configuration: PlanarConfiguration,
}

impl PredictorInfo<'_> {
    /// chunk width in pixels, taking padding into account
    ///
    /// strips are considered image-width chunks
    ///
    /// # Example
    ///
    /// ```rust
    /// # use async_tiff::tiff::tags::{SampleFormat, PlanarConfiguration};
    /// # use async_tiff::reader::Endianness;
    /// # use async_tiff::PredictorInfo;
    /// let info = PredictorInfo {
    /// # endianness: Endianness::LittleEndian,
    ///   image_width: 15,
    ///   image_height: 15,
    ///   chunk_width: 8,
    ///   chunk_height: 8,
    /// # bits_per_sample: &[32],
    /// # samples_per_pixel: 1,
    /// # sample_format: &[SampleFormat::IEEEFP],
    /// # planar_configuration: PlanarConfiguration::Chunky,
    /// };
    ///
    /// assert_eq!(info.chunk_width_pixels(0).unwrap(), (8));
    /// assert_eq!(info.chunk_width_pixels(1).unwrap(), (7));
    /// info.chunk_width_pixels(2).unwrap_err();
    /// ```
    pub fn chunk_width_pixels(&self, x: u32) -> AsyncTiffResult<u32> {
        if x >= self.chunks_across() {
            Err(crate::error::AsyncTiffError::TileIndexError(
                x,
                self.chunks_across(),
            ))
        } else if x == self.chunks_across() - 1 {
            // last chunk
            Ok(self.image_width - self.chunk_width * x)
        } else {
            Ok(self.chunk_width)
        }
    }

    /// chunk height in pixels, taking padding into account
    ///
    /// strips are considered image-width chunks
    ///
    /// # Example
    ///
    /// ```rust
    /// # use async_tiff::tiff::tags::{SampleFormat, PlanarConfiguration};
    /// # use async_tiff::reader::Endianness;
    /// # use async_tiff::PredictorInfo;
    /// let info = PredictorInfo {
    /// # endianness: Endianness::LittleEndian,
    ///   image_width: 15,
    ///   image_height: 15,
    ///   chunk_width: 8,
    ///   chunk_height: 8,
    /// # bits_per_sample: &[32],
    /// # samples_per_pixel: 1,
    /// # sample_format: &[SampleFormat::IEEEFP],
    /// # planar_configuration: PlanarConfiguration::Chunky,
    /// };
    ///
    /// assert_eq!(info.chunk_height_pixels(0).unwrap(), (8));
    /// assert_eq!(info.chunk_height_pixels(1).unwrap(), (7));
    /// info.chunk_height_pixels(2).unwrap_err();
    /// ```
    pub fn chunk_height_pixels(&self, y: u32) -> AsyncTiffResult<u32> {
        if y >= self.chunks_down() {
            Err(crate::error::AsyncTiffError::TileIndexError(
                y,
                self.chunks_down(),
            ))
        } else if y == self.chunks_down() - 1 {
            // last chunk
            Ok(self.image_height - self.chunk_height * y)
        } else {
            Ok(self.chunk_height)
        }
    }

    /// get the output row stride in bytes, taking padding into account
    pub fn output_row_stride(&self, x: u32) -> AsyncTiffResult<usize> {
        Ok((self.chunk_width_pixels(x)? as usize).saturating_mul(self.bits_per_pixel()) / 8)
    }

    /// The total number of bits per pixel, taking into account possible different sample sizes
    ///
    /// Technically bits_per_sample.len() should be *equal* to samples, but libtiff also allows
    /// it to be a single value that applies to all samples.
    ///
    /// Libtiff and image-tiff do not support mixed bits per sample, but we give the possibility
    /// unless you also have PlanarConfiguration::Planar, at which point the first is taken
    pub fn bits_per_pixel(&self) -> usize {
        match self.planar_configuration {
            PlanarConfiguration::Chunky => {
                if self.bits_per_sample.len() == 1 {
                    self.samples_per_pixel as usize * self.bits_per_sample[0] as usize
                } else {
                    assert_eq!(self.samples_per_pixel as usize, self.bits_per_sample.len());
                    self.bits_per_sample.iter().map(|v| *v as usize).product()
                }
            }
            PlanarConfiguration::Planar => self.bits_per_sample[0] as usize,
        }
    }

    /// The number of chunks in the horizontal (x) direction
    pub fn chunks_across(&self) -> u32 {
        self.image_width.div_ceil(self.chunk_width)
    }

    /// The number of chunks in the vertical (y) direction
    pub fn chunks_down(&self) -> u32 {
        self.image_height.div_ceil(self.chunk_height)
    }
}

/// A TIFF Tile response.
///
/// This contains the required information to decode the tile. Decoding is separated from fetching
/// so that sync and async operations can be separated and non-blocking.
///
/// This is returned by `fetch_tile`.
#[derive(Debug)]
pub struct Tile<'a> {
    pub(crate) x: usize,
    pub(crate) y: usize,
    pub(crate) predictor: Predictor,
    pub(crate) predictor_info: PredictorInfo<'a>,
    pub(crate) compressed_bytes: Bytes,
    pub(crate) compression_method: CompressionMethod,
    pub(crate) photometric_interpretation: PhotometricInterpretation,
    pub(crate) jpeg_tables: Option<Bytes>,
}

impl Tile<'_> {
    /// The column index of this tile.
    pub fn x(&self) -> usize {
        self.x
    }

    /// The row index of this tile.
    pub fn y(&self) -> usize {
        self.y
    }

    /// Access the compressed bytes underlying this tile.
    ///
    /// Note that [`Bytes`] is reference-counted, so it is very cheap to clone if needed.
    pub fn compressed_bytes(&self) -> &Bytes {
        &self.compressed_bytes
    }

    /// Access the compression tag representing this tile.
    pub fn compression_method(&self) -> CompressionMethod {
        self.compression_method
    }

    /// Access the photometric interpretation tag representing this tile.
    pub fn photometric_interpretation(&self) -> PhotometricInterpretation {
        self.photometric_interpretation
    }

    /// Access the JPEG Tables, if any, from the IFD producing this tile.
    ///
    /// Note that [`Bytes`] is reference-counted, so it is very cheap to clone if needed.
    pub fn jpeg_tables(&self) -> Option<&Bytes> {
        self.jpeg_tables.as_ref()
    }

    /// Decode this tile.
    ///
    /// Decoding is separate from fetching so that sync and async operations do not block the same
    /// runtime.
    pub fn decode(
        self,
        decoder_registry: &DecoderRegistry,
        predictor_registry: &RevPredictorRegistry,
    ) -> AsyncTiffResult<Bytes> {
        let decoder = decoder_registry
            .as_ref()
            .get(&self.compression_method)
            .ok_or(TiffError::UnsupportedError(
                TiffUnsupportedError::UnsupportedCompressionMethod(self.compression_method),
            ))?;

        let predictor =
            predictor_registry
                .as_ref()
                .get(&self.predictor)
                .ok_or(TiffError::UnsupportedError(
                    TiffUnsupportedError::UnsupportedPredictor(self.predictor),
                ))?;

        predictor.rev_predict_fix_endianness(
            decoder.decode_tile(
                self.compressed_bytes.clone(),
                self.photometric_interpretation,
                self.jpeg_tables.as_deref(),
            )?,
            &self.predictor_info,
            self.x as _,
            self.y as _,
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{
        reader::Endianness,
        tiff::tags::{PlanarConfiguration, SampleFormat},
    };

    use super::PredictorInfo;

    #[test]
    fn test_chunk_width_pixels() {
        let info = PredictorInfo {
            endianness: Endianness::LittleEndian,
            image_width: 15,
            image_height: 17,
            chunk_width: 8,
            chunk_height: 8,
            bits_per_sample: &[8],
            samples_per_pixel: 1,
            sample_format: &[SampleFormat::Uint],
            planar_configuration: PlanarConfiguration::Chunky,
        };
        assert_eq!(info.bits_per_pixel(), 8);
        assert_eq!(info.chunks_across(), 2);
        assert_eq!(info.chunks_down(), 3);
        assert_eq!(info.chunk_width_pixels(0).unwrap(), info.chunk_width);
        assert_eq!(info.chunk_width_pixels(1).unwrap(), 7);
        info.chunk_width_pixels(2).unwrap_err();
        assert_eq!(info.chunk_height_pixels(0).unwrap(), info.chunk_height);
        assert_eq!(info.chunk_height_pixels(2).unwrap(), 1);
        info.chunk_height_pixels(3).unwrap_err();
    }
}
