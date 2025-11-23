/// Empties `vec` and changes the lifetime of its future elements to
/// `'b`. This function should be optimized away by the compiler
/// except for the clear call.
#[inline(always)]
pub fn vec_change_lifetime_move<'a, 'b, T1: 'a, T2: 'b>(mut vec: Vec<T1>) -> Vec<T2> {
    vec.clear();
    vec.into_iter().map(|_| unreachable!()).collect()
}
