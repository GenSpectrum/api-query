// I did this before?

use num_traits::SaturatingAdd;

/// Automatically extending Vec
pub struct AutoVec<T> {
    fill: T,
    vec: Vec<T>,
}

impl<T: Clone + Copy> AutoVec<T> {
    pub fn new(fill: T) -> Self {
        Self {
            fill,
            vec: Vec::new(),
        }
    }

    pub fn set(&mut self, i: usize, val: T) {
        if self.vec.len() <= i {
            let fill = self.fill;
            self.vec.resize_with(i + 1, || fill);
        }
        self.vec[i] = val;
    }

    pub fn get_copy(&self, i: usize) -> T {
        self.vec.get(i).copied().unwrap_or(self.fill)
    }

    pub fn get_mut(&mut self, i: usize) -> &mut T {
        if self.vec.len() <= i {
            let fill = self.fill;
            self.vec.resize_with(i + 1, || fill);
        }
        &mut self.vec[i]
    }

    pub fn saturating_inc(&mut self, i: usize) -> T
    where
        T: SaturatingAdd + From<u8>,
    {
        let val = self.get_mut(i);
        let new_val = val.saturating_add(&T::from(1));
        *val = new_val;
        new_val
    }

    pub fn len(&self) -> usize {
        self.vec.len()
    }
}
