#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

pub mod reader;
// TODO: maybe rename this mod
mod cog;
pub mod decoder;
pub mod error;
pub mod geo;
mod ifd;
pub mod tiff;
mod tile;

#[cfg(not(feature = "object_store"))]
mod object_store;
#[cfg(feature = "object_store")]
pub use object_store::coalesce_ranges;
#[cfg(not(feature = "object_store"))]
pub use object_store::util::coalesce_ranges;

pub use cog::TIFF;
pub use ifd::{ImageFileDirectories, ImageFileDirectory};
pub use tile::Tile;
