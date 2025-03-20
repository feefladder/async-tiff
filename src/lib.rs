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

#[cfg(not(feature="object_store"))]
mod coalesce_ranges;
#[cfg(not(feature="object_store"))]
pub use coalesce_ranges::coalesce_ranges;
#[cfg(feature="object_store")]
pub use object_store::coalesce_ranges;

pub use cog::TIFF;
pub use ifd::{ImageFileDirectories, ImageFileDirectory};
pub use tile::Tile;
