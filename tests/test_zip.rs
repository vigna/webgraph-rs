/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::*;
use webgraph::graph::vec_graph::VecGraph;
use webgraph::traits::RandomAccessLabelling;
use webgraph::utils::Zip;

#[test]
fn test_zip() {
    let v = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0)]);
    let z = Zip::new(v.clone(), v.clone());
    let mut lender = z.into_lender();
    while let Some((x, i)) = lender.next() {
        let s = i.collect::<Vec<_>>();
        println!("{:?} {:?}", x, s);
        assert_eq!(z.successors(x).collect::<Vec<_>>(), s);
    }
}
