/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::*;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::labels::proj::LeftIntoIter;
use webgraph::labels::Zip;
use webgraph::traits::RandomAccessLabelling;

#[test]
fn test_zip() {
    let v = VecGraph::from_arc_list([(0, 1), (1, 2), (2, 0)]);
    let z = Zip(v.clone(), v.clone());
    let mut lender = z.into_lender();
    while let Some((x, i)) = lender.next() {
        let s = i.collect::<Vec<_>>();
        println!("{:?} {:?}", x, s);
        assert_eq!(z.labels(x).collect::<Vec<_>>(), s);
        assert_eq!(
            LeftIntoIter(z.labels(x)).into_iter().collect::<Vec<_>>(),
            v.labels(x).into_iter().collect::<Vec<_>>()
        )
    }
}
