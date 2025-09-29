/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use webgraph_algo::utils::math::*;

mod test_argmax {
    use super::*;

    #[test]
    fn test_empty() {
        let v: Vec<usize> = Vec::new();
        assert_eq!(argmax(&v), None);
    }

    #[test]
    fn test_single_element_min() {
        let v = vec![usize::MIN];
        assert_eq!(argmax(&v), Some(0));
    }

    #[test]
    fn test_normal() {
        let v = vec![2, 1, 5, 3];
        assert_eq!(argmax(&v), Some(2));
    }

    #[test]
    fn test_duplicates() {
        let v = vec![2, 5, 1, 3, 5];
        assert_eq!(argmax(&v), Some(1));
    }

    #[test]
    fn test_filtered_empty() {
        let v: Vec<usize> = Vec::new();
        let t: Vec<usize> = Vec::new();
        assert_eq!(argmax_filtered(&v, &t, |_, _| true), None);
    }

    #[test]
    fn test_all_filtered_away() {
        let v = vec![2, 1, 5, 3, 1];
        let t = vec![5, 4, 3, 2, 1];
        assert_eq!(argmax_filtered(&v, &t, |_, _| false), None);
    }

    #[test]
    fn test_filtered_single_element_min() {
        let v = vec![usize::MIN];
        let t = vec![usize::MIN];
        assert_eq!(argmax_filtered(&v, &t, |_, _| true), Some(0));
    }

    #[test]
    fn test_filtered_normal() {
        let v = vec![1, 2, 3, 4, 5, 4, 3, 2, 1];
        let t = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        assert_eq!(argmax_filtered(&v, &t, |_, e| *e < 4), Some(6));
    }

    #[test]
    fn test_filtered_duplicates() {
        let v = vec![1, 2, 3, 2, 1];
        let t = vec![1, 2, 3, 2, 1];
        assert_eq!(argmax_filtered(&v, &t, |_, e| *e < 3), Some(1));
    }
}

mod test_argmin {
    use super::*;

    #[test]
    fn test_empty() {
        let v: Vec<usize> = Vec::new();
        assert_eq!(argmin(&v), None);
    }

    #[test]
    fn test_single_element_max() {
        let v = vec![usize::MAX];
        assert_eq!(argmin(&v), Some(0));
    }

    #[test]
    fn test_normal() {
        let v = vec![2, 1, 5, 3];
        assert_eq!(argmin(&v), Some(1));
    }

    #[test]
    fn test_duplicates() {
        let v = vec![2, 1, 5, 3, 1];
        assert_eq!(argmin(&v), Some(1));
    }

    #[test]
    fn test_filtered_empty() {
        let v: Vec<usize> = Vec::new();
        let t: Vec<usize> = Vec::new();
        assert_eq!(argmin_filtered(&v, &t, |_, _| true), None);
    }

    #[test]
    fn test_all_filtered_away() {
        let v = vec![2, 1, 5, 3, 1];
        let t = vec![5, 4, 3, 2, 1];
        assert_eq!(argmin_filtered(&v, &t, |_, _| false), None);
    }

    #[test]
    fn test_filtered_single_element_max() {
        let v = vec![usize::MAX];
        let t = vec![usize::MAX];
        assert_eq!(argmin_filtered(&v, &t, |_, _| true), Some(0));
    }

    #[test]
    fn test_filtered_normal() {
        let v = vec![1, 2, 3, 4, 5, 4, 3, 2, 1];
        let t = vec![9, 8, 7, 6, 5, 4, 3, 2, 1];
        assert_eq!(argmin_filtered(&v, &t, |_, e| *e > 2), Some(6));
    }

    #[test]
    fn test_filtered_duplicates() {
        let v = vec![1, 2, 3, 2, 1];
        let t = vec![1, 2, 3, 2, 1];
        assert_eq!(argmin_filtered(&v, &t, |_, e| *e > 1), Some(1));
    }
}
