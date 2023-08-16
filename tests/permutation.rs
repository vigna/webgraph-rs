use rand::seq::SliceRandom;
use rand::Rng;

use webgraph::algorithms::{invert_permutation, invert_permutation_unchecked};

macro_rules! test_permut {
    ($order:expr, $permut:expr) => {
        assert_eq!(invert_permutation($order.into_iter()), Ok($permut));
        assert_eq!(
            unsafe { invert_permutation_unchecked($order.into_iter()) },
            $permut
        );

        // Check it is its own inverse
        assert_eq!(invert_permutation($permut.into_iter()), Ok($order));
        assert_eq!(
            unsafe { invert_permutation_unchecked($permut.into_iter()) },
            $order
        );
    };
}

#[test]
fn test_permutation_trivial() {
    test_permut!(vec![], vec![]);

    test_permut!(vec![0], vec![0]);

    test_permut!(vec![0, 1], vec![0, 1]);
    test_permut!(vec![1, 0], vec![1, 0]);

    assert_eq!(invert_permutation(vec![0, 0].into_iter()), Err(0));
    assert_eq!(invert_permutation(vec![1, 1].into_iter()), Err(1));
}

#[test]
fn test_permutation_three() {
    test_permut!(vec![0, 1, 2], vec![0, 1, 2]);
    test_permut!(vec![0, 2, 1], vec![0, 2, 1]);
    test_permut!(vec![1, 0, 2], vec![1, 0, 2]);
    test_permut!(vec![1, 2, 0], vec![2, 0, 1]);
    test_permut!(vec![2, 0, 1], vec![1, 2, 0]);
    test_permut!(vec![2, 1, 0], vec![2, 1, 0]);

    assert_eq!(invert_permutation(vec![0, 1, 0].into_iter()), Err(0));
    assert_eq!(invert_permutation(vec![1, 2, 2].into_iter()), Err(2));
    assert_eq!(invert_permutation(vec![2, 2, 1].into_iter()), Err(2));
}

#[test]
fn test_permutation_four() {
    test_permut!(vec![0, 1, 2, 3], vec![0, 1, 2, 3]);
    test_permut!(vec![0, 1, 3, 2], vec![0, 1, 3, 2]);
    test_permut!(vec![0, 2, 1, 3], vec![0, 2, 1, 3]);
    test_permut!(vec![0, 2, 3, 1], vec![0, 3, 1, 2]);
    test_permut!(vec![0, 3, 2, 1], vec![0, 3, 2, 1]);
    test_permut!(vec![0, 3, 1, 2], vec![0, 2, 3, 1]);

    test_permut!(vec![1, 2, 3, 0], vec![3, 0, 1, 2]);

    test_permut!(vec![2, 3, 0, 1], vec![2, 3, 0, 1]);

    test_permut!(vec![3, 0, 1, 2], vec![1, 2, 3, 0]);

    assert_eq!(invert_permutation(vec![0, 1, 2, 0].into_iter()), Err(0));
    assert_eq!(invert_permutation(vec![1, 2, 2, 3].into_iter()), Err(2));
    assert_eq!(invert_permutation(vec![2, 2, 3, 1].into_iter()), Err(2));
}

#[test]
fn test_permutation_random() {
    let mut rng = rand::thread_rng();

    for _ in 0..100 {
        let len = rng.gen_range(4..100);
        let mut order: Vec<_> = (0..len).collect();
        order.shuffle(&mut rng);

        assert_eq!(
            invert_permutation(
                invert_permutation(order.clone().into_iter())
                    .unwrap()
                    .into_iter()
            )
            .unwrap(),
            order
        );
        assert_eq!(
            invert_permutation(
                unsafe { invert_permutation_unchecked(order.clone().into_iter()) }.into_iter()
            )
            .unwrap(),
            order
        );
        assert_eq!(
            unsafe {
                invert_permutation_unchecked(
                    invert_permutation(order.clone().into_iter())
                        .unwrap()
                        .into_iter(),
                )
            },
            order
        );
        assert_eq!(
            unsafe {
                invert_permutation_unchecked(
                    invert_permutation_unchecked(order.clone().into_iter()).into_iter(),
                )
            },
            order
        );
    }
}
