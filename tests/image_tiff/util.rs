#[cfg(feature = "object_store")]
use async_tiff::reader::ObjectReader;
use async_tiff::TIFF;
#[cfg(feature = "object_store")]
use object_store::local::LocalFileSystem;
#[cfg(feature = "object_store")]
use std::env::current_dir;
use std::sync::Arc;

const TEST_IMAGE_DIR: &str = "tests/image_tiff/images/";

#[cfg(feature = "object_store")]
pub(crate) async fn open_tiff(filename: &str) -> TIFF {
    let store = Arc::new(LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap());
    let path = format!("{TEST_IMAGE_DIR}/{filename}");
    let reader = Arc::new(ObjectReader::new(store.clone(), path.as_str().into()));
    TIFF::try_open(reader).await.unwrap()
}

#[cfg(not(feature = "object_store"))]
pub(crate) async fn open_tiff(filename: &str) -> TIFF {
    // let store = Arc::new(LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap());
    let path = format!("{TEST_IMAGE_DIR}/{filename}");
    let reader = Arc::new(std::fs::File::open(path).expect("could not open file"));
    TIFF::try_open(reader).await.unwrap()
}
