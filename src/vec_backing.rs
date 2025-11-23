use std::ops::{Deref, DerefMut};

#[inline(always)]
unsafe fn vec_change_lifetime_mut<'hypothetical, 'real, 't: 'real, T: 'real + ?Sized>(
    vec: &'t mut Vec<&'hypothetical T>,
) -> &'t mut Vec<&'real T> {
    unsafe {
        let ptr: *mut Vec<&T> = vec;
        &mut *(ptr as *mut Vec<&T>)
    }
}

/// A Vec that can contain references but is normally empty, but
/// retains its backing storage, for efficient temporary re-use with
/// references with local lifetimes.
pub struct RefVecBacking<'t, T: ?Sized> {
    vec: Vec<&'t T>,
}

impl<'t, T: ?Sized> RefVecBacking<'t, T> {
    pub fn new() -> Self {
        Self { vec: Vec::new() }
    }

    pub fn borrow_mut<'newrf, 'newt: 'newrf>(
        &'newt mut self,
    ) -> RefVecBackingGuard<'newt, 'newrf, T> {
        RefVecBackingGuard {
            vec: unsafe {
                // Safe because Drop removes any elements, thus
                // nothing with lifetime 'newrf will remain in the Vec
                // while it can be accessible with other lifetimes.
                vec_change_lifetime_mut(&mut self.vec)
            },
        }
    }
}

pub struct RefVecBackingGuard<'t: 'newrf, 'newrf, T: ?Sized> {
    vec: &'t mut Vec<&'newrf T>,
}

impl<'t: 'newrf, 'newrf, T: ?Sized> Drop for RefVecBackingGuard<'t, 'newrf, T> {
    fn drop(&mut self) {
        self.vec.clear();
    }
}

impl<'t: 'newrf, 'newrf, T: ?Sized> Deref for RefVecBackingGuard<'t, 'newrf, T> {
    type Target = Vec<&'newrf T>;

    fn deref(&self) -> &Self::Target {
        self.vec
    }
}

impl<'t: 'newrf, 'newrf, T: ?Sized> DerefMut for RefVecBackingGuard<'t, 'newrf, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.vec
    }
}
