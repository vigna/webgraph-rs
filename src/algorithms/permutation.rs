unsafe fn invert_permutation_inner<O: ExactSizeIterator<Item = usize>>(
    order: O,
    permutation: &mut Vec<usize>,
    check_duplicates: bool,
) -> Result<(), usize> {
    let num_items = order.len();
    let mut actual_num_items = 0;
    for (position, node) in order.into_iter().enumerate() {
        assert!(node < num_items);
        assert!(position < num_items);
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
    unsafe { invert_permutation_inner(order, &mut permutation, true) }?;
    Ok(permutation)
}

/// Same as [`invert_permutation`] but returns initizalized memory if the
/// order contains duplicated elements.
///
/// # Panics
///
/// If [`ExactSizeIterator::len`] does not return the number of elements in `order`,
/// or if some elements are missing
pub unsafe fn invert_permutation_unchecked<O: ExactSizeIterator<Item = usize>>(
    order: O,
) -> Vec<usize> {
    let mut permutation = Vec::with_capacity(order.len());
    unsafe { permutation.set_len(order.len()) };
    unsafe { invert_permutation_inner(order, &mut permutation, false) }.unwrap();
    permutation
}
