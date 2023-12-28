/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::*;
use webgraph::graph::vec_graph::VecGraph;
use webgraph::traits::RandomAccessLabelling;
use webgraph::utils::proj::{Left, Right};
use webgraph::utils::Zip;

#[test]
fn test_left() {
    let v = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0)]);
    let z = Zip(v.clone(), v.clone());
    let p = Left(z);
    let mut lender = p.into_lender();
    while let Some((x, i)) = lender.next() {
        let s = i.into_iter().collect::<Vec<_>>();
        println!("{:?} {:?}", x, s);
        assert_eq!(p.successors(x).into_iter().collect::<Vec<_>>(), s);
        assert_eq!(v.successors(x).collect::<Vec<_>>(), s);
    }

    let p = Right(p.0);
    let mut lender = p.into_lender();
    while let Some((x, i)) = lender.next() {
        let s = i.into_iter().collect::<Vec<_>>();
        println!("{:?} {:?}", x, s);
        assert_eq!(p.successors(x).into_iter().collect::<Vec<_>>(), s);
        assert_eq!(v.successors(x).collect::<Vec<_>>(), s);
    }
}
