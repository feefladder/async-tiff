use crate::error::{AsyncTiffError, AsyncTiffResult};
use crate::tiff::tags::Tag;
use crate::tiff::Value;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

/// Trait to implement for custom tags, such as Geo, EXIF, OME, etc
/// your type should also implement `Clone`
// Send + Sync are required for Python, where `dyn ExtraTags` needs `Send` and `Sync`
pub trait ExtraTags: ExtraTagsCloneArc + std::any::Any + Debug + Send + Sync {
    /// a list of tags this entry processes
    /// e.g. for Geo this would be [34735, 34736, 34737]
    fn tags(&self) -> &'static [Tag];
    /// process a single tag
    fn process_tag(&mut self, tag: u16, value: Value) -> AsyncTiffResult<()>;
}

// we need to do a little dance to do an object-safe deep clone
// https://stackoverflow.com/a/30353928/14681457
pub trait ExtraTagsCloneArc {
    fn clone_arc(&self) -> Arc<dyn ExtraTags>;
}

impl<T> ExtraTagsCloneArc for T
where
    T: 'static + ExtraTags + Clone,
{
    fn clone_arc(&self) -> Arc<dyn ExtraTags> {
        Arc::new(self.clone())
    }
}

/// The registry in which extra tags (parsers) are registered
/// This is passed to TODO
#[derive(Debug, Clone)]
pub struct ExtraTagsRegistry(HashMap<Tag, Arc<dyn ExtraTags>>);

impl ExtraTagsRegistry {
    /// Create a new, empty `ExtraTagsRegistry`
    pub fn new() -> Self {
        Self(HashMap::new())
    }
    /// Register an ExtraTags so their tags are parsed and stored in the ifd's `extra_tags``
    pub fn register(&mut self, tags: Arc<dyn ExtraTags>) -> AsyncTiffResult<()> {
        // check for duplicates
        for tag in tags.tags() {
            if self.0.contains_key(tag) {
                return Err(AsyncTiffError::General(format!(
                    "Tag {tag:?} already registered in {self:?}!"
                )));
            }
        }
        // add to self
        for tag in tags.tags() {
            self.0.insert(*tag, tags.clone());
        }
        Ok(())
    }

    /// deep clone so we have different registries per IFD
    pub(crate) fn deep_clone(&self) -> Self {
        let mut new_registry = ExtraTagsRegistry::new();

        // we need to do some magic, because we can have multiple tags pointing to the same arc
        let mut seen = HashSet::new();
        for extra_tags in self.0.values() {
            // only add if this is the first encountered reference to this arc
            // (using thin pointer equality: https://stackoverflow.com/a/67114787/14681457 ; https://github.com/rust-lang/rust/issues/46139#issuecomment-346971153)
            if seen.insert(Arc::as_ptr(extra_tags) as *const ()) {
                if let Err(e) = new_registry.register(extra_tags.clone_arc()) {
                    panic!("{e}");
                }
            }
        }

        new_registry
    }
}

impl Default for ExtraTagsRegistry {
    fn default() -> Self {
        Self::new() // add e.g. geo tags later
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

    #[derive(Debug, Clone, PartialEq)]
    struct TestyTag;

    static TESTY_TAGS: LazyLock<Vec<Tag>> = LazyLock::new(|| {
        vec![
            Tag::from_u16_exhaustive(u16::MAX),
            Tag::from_u16_exhaustive(u16::MAX - 1),
        ]
    });

    impl ExtraTags for TestyTag {
        fn tags(&self) -> &'static [Tag] {
            &TESTY_TAGS
        }

        fn process_tag(
            &mut self,
            tag: u16,
            value: crate::tiff::Value,
        ) -> crate::error::AsyncTiffResult<()> {
            println!("received {tag:?}: {value:?}");
            Ok(())
        }
    }

    #[test]
    fn test_register() {
        let mut registry = ExtraTagsRegistry::new();
        assert!(registry.0.is_empty());
        let a1: Arc<dyn ExtraTags> = Arc::new(TestyTag);
        registry.register(a1.clone()).unwrap();
        assert_eq!(registry.0.len(), TestyTag.tags().len());
        for tag in a1.tags() {
            // very strict equality check
            assert!(Arc::ptr_eq(&registry.0[tag], &a1));
        }
    }

    #[test]
    fn test_overlap_err() {
        let mut registry = ExtraTagsRegistry::new();
        assert!(registry.0.is_empty());
        registry.register(Arc::new(TestyTag)).unwrap();
        assert!(matches!(
            registry.register(Arc::new(TestyTag)).unwrap_err(),
            AsyncTiffError::General(_)
        ));
    }

    #[test]
    fn test_deep_clone() {
        let mut registry = ExtraTagsRegistry::new();
        let a1: Arc<dyn ExtraTags> = Arc::new(TestyTag);
        registry.register(a1.clone()).unwrap();
        let r2 = registry.deep_clone();
        for tags in a1.tags().windows(2) {
            // all should refer to the same Arc
            assert!(Arc::ptr_eq(&r2.0[&tags[0]], &r2.0[&tags[1]]));
            // which is different from the previous
            assert!(!Arc::ptr_eq(&a1, &r2.0[&tags[0]]));
            assert!(!Arc::ptr_eq(&a1, &r2.0[&tags[1]]));
        }
    }
}
