use async_tiff::TIFF;
use std::sync::Arc;

#[cfg(feature = "object_store")]
use async_tiff::reader::ObjectReader;
#[cfg(feature = "object_store")]
use object_store::local::LocalFileSystem;
#[cfg(feature = "object_store")]
use std::env::current_dir;

#[cfg(not(any(feature = "tokio", feature = "object_store")))]
use async_tiff::{
    error::{AsyncTiffError, AsyncTiffResult},
    reader::AsyncFileReader,
};
#[cfg(not(any(feature = "tokio", feature = "object_store")))]
use bytes::Bytes;
#[cfg(not(any(feature = "tokio", feature = "object_store")))]
use futures::{future::BoxFuture, FutureExt};
#[cfg(not(any(feature = "tokio", feature = "object_store")))]
use std::ops::Range;

const TEST_IMAGE_DIR: &str = "tests/image_tiff/images/";

#[cfg(feature = "object_store")]
pub(crate) async fn open_tiff(filename: &str) -> TIFF {
    let store = Arc::new(LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap());
    let path = format!("{TEST_IMAGE_DIR}/{filename}");
    let reader = Arc::new(ObjectReader::new(store.clone(), path.as_str().into()));
    TIFF::try_open(reader).await.unwrap()
}

#[cfg(all(feature = "tokio", not(feature = "object_store")))]
pub(crate) async fn open_tiff(filename: &str) -> TIFF {
    // let store = Arc::new(LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap());
    let path = format!("{TEST_IMAGE_DIR}/{filename}");
    let reader = Arc::new(
        tokio::fs::File::open(path)
            .await
            .expect("could not open file"),
    );
    TIFF::try_open(reader).await.unwrap()
}

#[cfg(not(any(feature = "tokio", feature = "object_store")))]
#[derive(Debug)]
struct TokioFile(tokio::fs::File);
#[cfg(not(any(feature = "tokio", feature = "object_store")))]
impl AsyncFileReader for TokioFile {
    fn get_bytes(&self, range: Range<u64>) -> BoxFuture<'_, AsyncTiffResult<Bytes>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        async move {
            let mut file = self.0.try_clone().await?;
            file.seek(std::io::SeekFrom::Start(range.start)).await?;

            let to_read = (range.end - range.start).try_into().unwrap();
            let mut buffer = Vec::with_capacity(to_read);
            let read = file.take(to_read as u64).read_to_end(&mut buffer).await?;
            if read != to_read {
                return Err(AsyncTiffError::EndOfFile(to_read, read));
            }

            Ok(buffer.into())
        }
        .boxed()
    }
}
#[cfg(not(any(feature = "tokio", feature = "object_store")))]
pub(crate) async fn open_tiff(filename: &str) -> TIFF {
    let path = format!("{TEST_IMAGE_DIR}/{filename}");
    let reader = Arc::new(TokioFile(
        tokio::fs::File::open(path)
            .await
            .expect("could not open file"),
    ));
    TIFF::try_open(reader).await.unwrap()
}
