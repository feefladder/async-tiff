#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

pub mod reader;
// TODO: maybe rename this mod
pub(crate) mod bytecast;
mod cog;
pub mod decoder;
mod decoding_result;
pub mod error;
pub mod geo;
mod ifd;
pub mod metadata;
pub mod predictor;
pub mod tiff;
mod tile;

pub use cog::TIFF;
pub use decoding_result::DecodingResult;
pub use ifd::ImageFileDirectory;
pub use tile::Tile;
