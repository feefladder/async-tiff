//! Predictors for no predictor, horizontal and floating-point
use std::collections::HashMap;
use std::fmt::Debug;

use bytes::{Bytes, BytesMut};
// use tiff::decoder::DecodingResult;

use crate::{
    error::AsyncTiffResult, reader::Endianness, tiff::tags::Predictor, tile::PredictorInfo,
};

/// A registry for reverse predictors
///
/// Reverse predictors, because they perform the inverse (decoding) operation of prediction
///
///
///
pub struct RevPredictorRegistry(HashMap<Predictor, Box<dyn RevPredict>>);

impl RevPredictorRegistry {
    /// create a new predictor registry with no predictors registered
    pub fn new() -> Self {
        Self(HashMap::new())
    }
}

impl AsRef<HashMap<Predictor, Box<dyn RevPredict>>> for RevPredictorRegistry {
    fn as_ref(&self) -> &HashMap<Predictor, Box<dyn RevPredict>> {
        &self.0
    }
}

impl Default for RevPredictorRegistry {
    fn default() -> Self {
        let mut hmap = HashMap::new();
        hmap.insert(Predictor::None, Box::new(NoPredictor) as _);
        hmap.insert(Predictor::Horizontal, Box::new(RevHorizontalPredictor) as _);
        hmap.insert(
            Predictor::FloatingPoint,
            Box::new(RevFloatingPointPredictor) as _,
        );
        Self(hmap)
    }
}

/// Trait for reverse predictors to implement
///
///
pub trait RevPredict: Debug + Send + Sync {
    /// reverse predict the decompressed bytes and fix endianness on the output
    ///
    ///
    fn rev_predict_fix_endianness(
        &self,
        buffer: Bytes,
        predictor_info: &PredictorInfo,
        tile_x: u32,
        tile_y: u32,
    ) -> AsyncTiffResult<Bytes>; // having this Bytes will give alignment issues later on
}

/// no predictor
#[derive(Debug)]
pub struct NoPredictor;

impl RevPredict for NoPredictor {
    fn rev_predict_fix_endianness(
        &self,
        buffer: Bytes,
        predictor_info: &PredictorInfo,
        _: u32,
        _: u32,
    ) -> AsyncTiffResult<Bytes> {
        let mut res = BytesMut::from(buffer);
        fix_endianness(
            &mut res[..],
            predictor_info.endianness,
            predictor_info.bits_per_sample[0],
        );
        Ok(res.into())
    }
}

/// reverse horizontal predictor
#[derive(Debug)]
pub struct RevHorizontalPredictor;

impl RevPredict for RevHorizontalPredictor {
    fn rev_predict_fix_endianness(
        &self,
        buffer: Bytes,
        predictor_info: &PredictorInfo,
        tile_x: u32,
        _: u32,
    ) -> AsyncTiffResult<Bytes> {
        let output_row_stride = predictor_info.output_row_stride(tile_x)?;
        let samples = predictor_info.samples_per_pixel as usize;
        let bit_depth = predictor_info.bits_per_sample[0];

        let mut res = BytesMut::from(buffer);
        fix_endianness(&mut res[..], predictor_info.endianness, bit_depth);
        for buf in res.chunks_mut(output_row_stride) {
            rev_hpredict_nsamp(buf, bit_depth, samples);
        }
        Ok(res.into())
    }
}

/// Reverse predictor convenienve function for horizontal differencing
///
/// From image-tiff
pub fn rev_hpredict_nsamp(buf: &mut [u8], bit_depth: u16, samples: usize) {
    match bit_depth {
        0..=8 => {
            for i in samples..buf.len() {
                buf[i] = buf[i].wrapping_add(buf[i - samples]);
            }
        }
        9..=16 => {
            for i in (samples * 2..buf.len()).step_by(2) {
                let v = u16::from_ne_bytes(buf[i..][..2].try_into().unwrap());
                let p = u16::from_ne_bytes(buf[i - 2 * samples..][..2].try_into().unwrap());
                buf[i..][..2].copy_from_slice(&(v.wrapping_add(p)).to_ne_bytes());
            }
        }
        17..=32 => {
            for i in (samples * 4..buf.len()).step_by(4) {
                let v = u32::from_ne_bytes(buf[i..][..4].try_into().unwrap());
                let p = u32::from_ne_bytes(buf[i - 4 * samples..][..4].try_into().unwrap());
                buf[i..][..4].copy_from_slice(&(v.wrapping_add(p)).to_ne_bytes());
            }
        }
        33..=64 => {
            for i in (samples * 8..buf.len()).step_by(8) {
                let v = u64::from_ne_bytes(buf[i..][..8].try_into().unwrap());
                let p = u64::from_ne_bytes(buf[i - 8 * samples..][..8].try_into().unwrap());
                buf[i..][..8].copy_from_slice(&(v.wrapping_add(p)).to_ne_bytes());
            }
        }
        _ => {
            unreachable!("Caller should have validated arguments. Please file a bug.")
        }
    }
}

/// Fix endianness. If `byte_order` matches the host, then conversion is a no-op.
///
/// from image-tiff
pub fn fix_endianness(buf: &mut [u8], byte_order: Endianness, bit_depth: u16) {
    match byte_order {
        Endianness::LittleEndian => match bit_depth {
            0..=8 => {}
            9..=16 => buf.chunks_exact_mut(2).for_each(|v| {
                v.copy_from_slice(&u16::from_le_bytes((*v).try_into().unwrap()).to_ne_bytes())
            }),
            17..=32 => buf.chunks_exact_mut(4).for_each(|v| {
                v.copy_from_slice(&u32::from_le_bytes((*v).try_into().unwrap()).to_ne_bytes())
            }),
            _ => buf.chunks_exact_mut(8).for_each(|v| {
                v.copy_from_slice(&u64::from_le_bytes((*v).try_into().unwrap()).to_ne_bytes())
            }),
        },
        Endianness::BigEndian => match bit_depth {
            0..=8 => {}
            9..=16 => buf.chunks_exact_mut(2).for_each(|v| {
                v.copy_from_slice(&u16::from_be_bytes((*v).try_into().unwrap()).to_ne_bytes())
            }),
            17..=32 => buf.chunks_exact_mut(4).for_each(|v| {
                v.copy_from_slice(&u32::from_be_bytes((*v).try_into().unwrap()).to_ne_bytes())
            }),
            _ => buf.chunks_exact_mut(8).for_each(|v| {
                v.copy_from_slice(&u64::from_be_bytes((*v).try_into().unwrap()).to_ne_bytes())
            }),
        },
    };
}

/// Floating point predictor
#[derive(Debug)]
pub struct RevFloatingPointPredictor;

impl RevPredict for RevFloatingPointPredictor {
    fn rev_predict_fix_endianness(
        &self,
        buffer: Bytes,
        predictor_info: &PredictorInfo,
        tile_x: u32,
        tile_y: u32,
    ) -> AsyncTiffResult<Bytes> {
        let output_row_stride = predictor_info.output_row_stride(tile_x)?;
        let mut res: BytesMut = BytesMut::zeroed(
            output_row_stride * predictor_info.chunk_height_pixels(tile_y)? as usize,
        );
        let bit_depth = predictor_info.bits_per_sample[0] as usize;
        if predictor_info.chunk_width_pixels(tile_x)? == predictor_info.chunk_width {
            // no special padding handling
            let mut input = BytesMut::from(buffer);
            for (in_buf, out_buf) in input
                .chunks_mut(output_row_stride)
                .zip(res.chunks_mut(output_row_stride))
            {
                match bit_depth {
                    16 => rev_predict_f16(in_buf, out_buf, predictor_info.samples_per_pixel as _),
                    32 => rev_predict_f32(in_buf, out_buf, predictor_info.samples_per_pixel as _),
                    64 => rev_predict_f64(in_buf, out_buf, predictor_info.samples_per_pixel as _),
                    _ => panic!("thou shalt not predict with float16"),
                }
            }
        } else {
            // specially handle padding bytes
            // create a buffer for the full width
            let mut input = BytesMut::from(buffer);

            let input_row_stride =
                predictor_info.chunk_width as usize * predictor_info.bits_per_pixel() / 8;
            for (in_buf, out_buf) in input
                .chunks_mut(input_row_stride)
                .zip(res.chunks_mut(output_row_stride))
            {
                let mut out_row = BytesMut::zeroed(input_row_stride);
                match bit_depth {
                    16 => rev_predict_f16(in_buf, out_buf, predictor_info.samples_per_pixel as _),
                    32 => {
                        rev_predict_f32(in_buf, &mut out_row, predictor_info.samples_per_pixel as _)
                    }
                    64 => {
                        rev_predict_f64(in_buf, &mut out_row, predictor_info.samples_per_pixel as _)
                    }
                    _ => panic!("thou shalt not predict f16"),
                }
                out_buf.copy_from_slice(&out_row[..output_row_stride]);
            }
        }
        Ok(res.into())
    }
}

/// Reverse floating point prediction
///
/// floating point prediction first shuffles the bytes and then uses horizontal
/// differencing  
/// also performs byte-order conversion if needed.
///
pub fn rev_predict_f16(input: &mut [u8], output: &mut [u8], samples: usize) {
    // reverse horizontal differencing
    for i in samples..input.len() {
        input[i] = input[i].wrapping_add(input[i - samples]);
    }
    // reverse byte shuffle and fix endianness
    for (i, chunk) in output.chunks_mut(2).enumerate() {
        chunk.copy_from_slice(&u16::to_ne_bytes(
            // convert to native-endian
            // preserve original byte-order
            u16::from_be_bytes([input[i], input[input.len() / 2 + i]]),
        ));
    }
}

/// Reverse floating point prediction
///
/// floating point prediction first shuffles the bytes and then uses horizontal
/// differencing  
/// also performs byte-order conversion if needed.
///
pub fn rev_predict_f32(input: &mut [u8], output: &mut [u8], samples: usize) {
    // reverse horizontal differencing
    for i in samples..input.len() {
        input[i] = input[i].wrapping_add(input[i - samples]);
    }
    println!("output: {output:?}, {:?}", output.len());
    // reverse byte shuffle and fix endianness
    for (i, chunk) in output.chunks_mut(4).enumerate() {
        println!("i:{i:?}");
        chunk.copy_from_slice(&u32::to_ne_bytes(
            // convert to native-endian
            // preserve original byte-order
            u32::from_be_bytes([
                input[i],
                input[input.len() / 4 + i],
                input[input.len() / 4 * 2 + i],
                input[input.len() / 4 * 3 + i],
            ]),
        ));
    }
}

/// Reverse floating point prediction
///
/// floating point prediction first shuffles the bytes and then uses horizontal
/// differencing  
/// Also fixes byte order if needed (tiff's->native)
pub fn rev_predict_f64(input: &mut [u8], output: &mut [u8], samples: usize) {
    for i in samples..input.len() {
        input[i] = input[i].wrapping_add(input[i - samples]);
    }

    for (i, chunk) in output.chunks_mut(8).enumerate() {
        chunk.copy_from_slice(&u64::to_ne_bytes(u64::from_be_bytes([
            input[i],
            input[input.len() / 8 + i],
            input[input.len() / 8 * 2 + i],
            input[input.len() / 8 * 3 + i],
            input[input.len() / 8 * 4 + i],
            input[input.len() / 8 * 5 + i],
            input[input.len() / 8 * 6 + i],
            input[input.len() / 8 * 7 + i],
        ])));
    }
}

#[cfg(test)]
mod test {
    use std::vec;

    use bytes::Bytes;

    use crate::{
        predictor::RevFloatingPointPredictor,
        reader::Endianness,
        tiff::tags::{PlanarConfiguration, SampleFormat},
        tile::PredictorInfo,
    };

    use super::{NoPredictor, RevHorizontalPredictor, RevPredict};

    const PRED_INFO: PredictorInfo = PredictorInfo {
        endianness: Endianness::LittleEndian,
        image_width: 7,
        image_height: 7,
        chunk_width: 4,
        chunk_height: 4,
        bits_per_sample: &[8],
        samples_per_pixel: 1,
        sample_format: &[SampleFormat::Uint],
        planar_configuration: crate::tiff::tags::PlanarConfiguration::Chunky,
    };
    #[rustfmt::skip]
    const RES: [u8;16] = [
        0,1, 2,3,
        1,0, 1,2,

        2,1, 0,1,
        3,2, 1,0,
        ];
    #[rustfmt::skip]
    const RES_RIGHT: [u8;12] = [
        0,1, 2,
        1,0, 1,

        2,1, 0,
        3,2, 1,
        ];
    #[rustfmt::skip]
    const RES_BOT: [u8;12] = [
        0,1,2, 3,
        1,0,1, 2,

        2,1,0, 1,
        ];
    #[rustfmt::skip]
    const RES_BOT_RIGHT: [u8;9] = [
        0,1, 2,
        1,0, 1,

        2,1, 0,
        ];

    #[rustfmt::skip]
    #[test]
    fn test_no_predict() {
        let p = NoPredictor;
        let cases = [
            (0,0, Bytes::from_static(&RES[..]),           Bytes::from_static(&RES[..])          ),
            (0,1, Bytes::from_static(&RES_BOT[..]),       Bytes::from_static(&RES_BOT[..])      ),
            (1,0, Bytes::from_static(&RES_RIGHT[..]),     Bytes::from_static(&RES_RIGHT[..])    ),
            (1,1, Bytes::from_static(&RES_BOT_RIGHT[..]), Bytes::from_static(&RES_BOT_RIGHT[..]))
        ];
        for (x,y, input, expected) in cases {
            assert_eq!(p.rev_predict_fix_endianness(input, &PRED_INFO, x, y).unwrap(), expected);
        }
    }

    #[rustfmt::skip]
    #[test]
    fn test_hpredict() {
        let p = RevHorizontalPredictor;
        let mut predictor_info = PRED_INFO.clone();
        let cases = [
            (0,0, vec![
                0i32, 1, 1, 1,
                1,-1, 1, 1,
                2,-1,-1, 1,
                3,-1,-1,-1,
            ], Vec::from(&RES[..])),
            (0,1, vec![
                0, 1, 1, 1,
                1,-1, 1, 1,
                2,-1,-1, 1,
            ], Vec::from(&RES_BOT[..])),
            (1,0, vec![
                0, 1, 1,
                1,-1, 1,
                2,-1,-1,
                3,-1,-1,
            ], Vec::from(&RES_RIGHT[..])),
            (1,1, vec![
                0, 1, 1,
                1,-1, 1,
                2,-1,-1,
            ], Vec::from(&RES_BOT_RIGHT[..])),
        ];
        for (x,y, input, expected) in cases {
            println!("uints littleendian");
            predictor_info.endianness = Endianness::LittleEndian;
            predictor_info.sample_format = &[SampleFormat::Uint];
            predictor_info.bits_per_sample = &[8];
            assert_eq!(-1i32 as u8, 255);
            println!("testing u8");
            let buffer = Bytes::from(input.iter().map(|v| *v as u8).collect::<Vec<_>>());
            let res = Bytes::from(expected.clone());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
            assert_eq!(-1i32 as u16, u16::MAX);
            println!("testing u16");
            predictor_info.bits_per_sample = &[16];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as u16).to_le_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as u16).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
            assert_eq!(-1i32 as u32, u32::MAX);
            println!("testing u32");
            predictor_info.bits_per_sample = &[32];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as u32).to_le_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as u32).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
            assert_eq!(-1i32 as u64, u64::MAX);
            println!("testing u64");
            predictor_info.bits_per_sample = &[64];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as u64).to_le_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as u64).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);

            println!("ints littleendian");
            predictor_info.sample_format = &[SampleFormat::Int];
            predictor_info.bits_per_sample = &[8];
            println!("testing i8");
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as i8).to_le_bytes()).collect::<Vec<_>>());
            println!("{:?}", &buffer[..]);
            let res = Bytes::from(expected.clone());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap()[..], res[..]);
            println!("testing i16");
            predictor_info.bits_per_sample = &[16];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as i16).to_le_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as i16).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
            println!("testing i32");
            predictor_info.bits_per_sample = &[32];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as i32).to_le_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as i32).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
            println!("testing i64");
            predictor_info.bits_per_sample = &[64];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as i64).to_le_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as i64).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);

            println!("uints bigendian");
            predictor_info.endianness = Endianness::BigEndian;
            predictor_info.sample_format = &[SampleFormat::Uint];
            predictor_info.bits_per_sample = &[8];
            assert_eq!(-1i32 as u8, 255);
            println!("testing u8");
            let buffer = Bytes::from(input.iter().map(|v| *v as u8).collect::<Vec<_>>());
            let res = Bytes::from(expected.clone());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
            assert_eq!(-1i32 as u16, u16::MAX);
            println!("testing u16");
            predictor_info.bits_per_sample = &[16];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as u16).to_be_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as u16).to_ne_bytes()).collect::<Vec<_>>());
            println!("buffer: {:?}", &buffer[..]);
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap()[..], res[..]);
            assert_eq!(-1i32 as u32, u32::MAX);
            println!("testing u32");
            predictor_info.bits_per_sample = &[32];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as u32).to_be_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as u32).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
            assert_eq!(-1i32 as u64, u64::MAX);
            println!("testing u64");
            predictor_info.bits_per_sample = &[64];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as u64).to_be_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as u64).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);

            println!("ints bigendian");
            predictor_info.sample_format = &[SampleFormat::Int];
            predictor_info.bits_per_sample = &[8];
            assert_eq!(-1i32 as u8, 255);
            println!("testing i8");
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as i8).to_be_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.clone());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
            assert_eq!(-1i32 as u16, u16::MAX);
            println!("testing i16");
            predictor_info.bits_per_sample = &[16];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as i16).to_be_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as i16).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
            assert_eq!(-1i32 as u32, u32::MAX);
            println!("testing i32");
            predictor_info.bits_per_sample = &[32];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as i32).to_be_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as i32).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
            assert_eq!(-1i32 as u64, u64::MAX);
            println!("testing i64");
            predictor_info.bits_per_sample = &[64];
            let buffer = Bytes::from(input.iter().flat_map(|v| (*v as i64).to_be_bytes()).collect::<Vec<_>>());
            let res = Bytes::from(expected.iter().flat_map(|v| (*v as i64).to_ne_bytes()).collect::<Vec<_>>());
            assert_eq!(p.rev_predict_fix_endianness(buffer, &predictor_info, x, y).unwrap(), res);
        }
    }

    // #[rustfmt::skip]
    #[test]
    fn test_fpredict_f32() {
        // let's take this 2-value image
        let expected: Vec<u8> = [42.0f32, 43.0]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        assert_eq!(expected, vec![0x0, 0x0, 0x28, 0x42, 0x0, 0x0, 0x2c, 0x42]);
        let info = PredictorInfo {
            endianness: Endianness::LittleEndian,
            image_width: 2,
            image_height: 2 + 1,
            chunk_width: 2,
            chunk_height: 2,
            bits_per_sample: &[32],
            samples_per_pixel: 1,
            sample_format: &[SampleFormat::IEEEFP],
            planar_configuration: PlanarConfiguration::Chunky,
        };
        let input = Bytes::from_static(&[0x42u8, 0, 230, 4, 212, 0, 0, 0]);
        assert_eq!(
            RevFloatingPointPredictor
                .rev_predict_fix_endianness(input, &info, 0, 1)
                .unwrap(),
            expected
        );
    }

    // #[test]
    // fn test_fpredict_f64() {
    //     // let's take this 2-value image
    //     let expected: Vec<u8> = [42.0f64, 43.0].iter().flat_map(|f| f.to_le_bytes()).collect();
    //     assert_eq!(expected, vec![0,0,0,0,0,0,69,64,0,0,0,0,0,128,69,64]);
    //     let info = PredictorInfo {
    //         endianness: Endianness::LittleEndian,
    //         image_width: 2,
    //         image_height: 2 + 1,
    //         chunk_width: 2,
    //         chunk_height: 2,
    //         bits_per_sample: &[64],
    //         samples_per_pixel: 1,
    //         sample_format: &[SampleFormat::IEEEFP],
    //         planar_configuration: PlanarConfiguration::Chunky,
    //     };
    //     let input = Bytes::from_static(&[0x42u8, 0, 230, 4, 212, 0, 0, 0]);
    //     assert_eq!(
    //         RevFloatingPointPredictor
    //             .rev_predict_fix_endianness(input, &info, 0, 1)
    //             .unwrap(),
    //         expected
    //     );
    // }
}
