use std::alloc::Layout;
use std::ptr::NonNull;

use rkyv::ser::serializers::{AllocScratch, BufferScratch, HeapScratch};
use rkyv::ser::ScratchSpace;
use rkyv::{AlignedBytes, Fallible};

const STACK_SCRATCH_SIZE: usize = 1024;
const HEAP_SCRATCH_SIZE: usize = 16 << 10;

#[derive(Debug)]
/// Allocates scratch space with a stack, fallback heap scratch, and then an alloc scratch.
///
/// The fallback heap scratch is lazily allocated.
pub struct LazyScratch {
    stack_scratch: StackScratch,
    heap_scratch: Option<HeapScratch<HEAP_SCRATCH_SIZE>>,
    alloc_scratch: AllocScratch,
}

impl Default for LazyScratch {
    fn default() -> Self {
        Self {
            stack_scratch: StackScratch::default(),
            heap_scratch: None,
            alloc_scratch: AllocScratch::default(),
        }
    }
}

impl Fallible for LazyScratch {
    type Error = <AllocScratch as Fallible>::Error;
}

impl ScratchSpace for LazyScratch {
    #[inline]
    unsafe fn push_scratch(
        &mut self,
        layout: Layout,
    ) -> Result<NonNull<[u8]>, Self::Error> {
        if let Ok(buf) = self.stack_scratch.push_scratch(layout) {
            return Ok(buf);
        }

        let heap_scratch = self.heap_scratch.get_or_insert_with(HeapScratch::new);

        if let Ok(buf) = heap_scratch.push_scratch(layout) {
            return Ok(buf);
        }

        self.alloc_scratch.push_scratch(layout)
    }

    #[inline]
    unsafe fn pop_scratch(
        &mut self,
        ptr: NonNull<u8>,
        layout: Layout,
    ) -> Result<(), Self::Error> {
        if self.stack_scratch.pop_scratch(ptr, layout).is_ok() {
            return Ok(());
        }

        let heap_scratch = self.heap_scratch.get_or_insert_with(HeapScratch::new);

        if heap_scratch.pop_scratch(ptr, layout).is_ok() {
            return Ok(());
        }

        self.alloc_scratch.pop_scratch(ptr, layout)
    }
}

#[derive(Debug)]
/// A stack allocated scratch space.
struct StackScratch {
    inner: BufferScratch<AlignedBytes<STACK_SCRATCH_SIZE>>,
}

impl Default for StackScratch {
    #[inline]
    fn default() -> Self {
        Self {
            inner: BufferScratch::new(AlignedBytes::default()),
        }
    }
}

impl Fallible for StackScratch {
    type Error = <BufferScratch<AlignedBytes<STACK_SCRATCH_SIZE>> as Fallible>::Error;
}

impl ScratchSpace for StackScratch {
    #[inline]
    unsafe fn push_scratch(
        &mut self,
        layout: Layout,
    ) -> Result<NonNull<[u8]>, Self::Error> {
        self.inner.push_scratch(layout)
    }

    #[inline]
    unsafe fn pop_scratch(
        &mut self,
        ptr: NonNull<u8>,
        layout: Layout,
    ) -> Result<(), Self::Error> {
        self.inner.pop_scratch(ptr, layout)
    }
}
