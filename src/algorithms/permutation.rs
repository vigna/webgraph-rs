unsafe fn invert_permutation_inner<O: ExactSizeIterator<Item = usize>>(
    order: O,
    permutation: &mut [usize],
    check_duplicates: bool,
) -> Result<(), usize> {
    let num_items = order.len();
    let mut actual_num_items = 0;
    for (position, node) in order.into_iter().enumerate() {
        assert!(node < num_items);
        assert!(position < num_items);

        // Safe because of the assertion above.
        if check_duplicates && unsafe { permutation.get_unchecked(node) } != &usize::MAX {
            return Err(node);
        }
        permutation[node] = position;
        actual_num_items = position + 1;
    }

    assert_eq!(
        num_items, actual_num_items,
        "Expected {} items, got {}",
        num_items, actual_num_items
    );

    Ok(())
}

/// Given a vector of unique node ids (interpreted as a position -> node map),
/// returns the inverted permutation (interpreted as a node -> position map)
///
/// Returns `Err(id)` if any id is duplicated.
///
/// # Panics
///
/// If [`ExactSizeIterator::len`] does not return the number of elements in `order`,
/// or if some elements are missing
pub fn invert_permutation<O: ExactSizeIterator<Item = usize>>(
    order: O,
) -> Result<Vec<usize>, usize> {
    let mut permutation = vec![usize::MAX; order.len()];
    unsafe { invert_permutation_inner(order, &mut permutation[..], true) }?;
    Ok(permutation)
}

/// Same as [`invert_permutation`] but returns initizalized memory if the
/// order contains duplicated elements.
///
/// # Safety
///
/// Returns uninitialized memory if any id is duplicated.
///
/// # Panics
///
/// If [`ExactSizeIterator::len`] does not return the number of elements in `order`,
/// or if some elements are missing
#[allow(clippy::uninit_vec)]
pub unsafe fn invert_permutation_unchecked<O: ExactSizeIterator<Item = usize>>(
    order: O,
) -> Vec<usize> {
    let mut permutation = Vec::with_capacity(order.len());
    unsafe { permutation.set_len(order.len()) };

    // .unwrap() because it is unchecked so it can't return Err
    invert_permutation_into_unchecked(order, &mut permutation[..]).unwrap();

    permutation
}

/// Same as [`invert_permutation_unchecked`], but writes to a slice instead of
/// returning a vector.
///
/// Returns `Err(id)` if any id is duplicated.
///
/// # Safety
///
/// This function is technically safe as it does not leave uninitialized memory
/// uninitialized.
///
/// However, if it it called with uninitialized memory, it leaves it uninitialized
/// if any id is duplicated.
///
/// # Panics
///
/// If [`ExactSizeIterator::len`] does not return the number of elements in `order`,
/// some elements are missing, or the `order` and `permutation` don't have the same length
pub fn invert_permutation_into_unchecked<O: ExactSizeIterator<Item = usize>>(
    order: O,
    permutation: &mut [usize],
) -> Result<(), usize> {
    assert_eq!(order.len(), permutation.len());
    unsafe { invert_permutation_inner(order, permutation, false) }
}
