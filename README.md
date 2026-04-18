# WebGraph

A Rust implementation of the [WebGraph framework] for graph compression.

## At a Glance

- Compressed graph representation (start from here):
  [`webgraph`] ([repo][webgraph repo])

- Algorithms: [`webgraph-algo`] ([repo][algo repo])

- CLI commands: [`webgraph-cli`] ([repo][cli repo])

## Users of WebGraph

<a href="https://www.softwareheritage.org/"><img src="svg/SWH.svg" width="200"></a>
        
<a href="https://www.commoncrawl.org/"><img src="svg/CC.svg" width="200"></a>

## Python

There are [Python bindings](https://pypi.org/project/webgraph/) for WebGraph.

# Papers

* A [detailed description](http://vigna.di.unimi.it/papers.php#BoVWFI) of
the compression algorithms used in WebGraph, published in the proceedings
of the [Thirteenth International World–Wide Web
Conference](http://www2004.org).

* A [mathematical analysis](http://vigna.di.unimi.it/papers.php#BoVCWWW)
of the performance of γ, δ and ζ codes against power-law distributions.

* Some [quite surprising
experiments](http://vigna.di.unimi.it/papers.php#BSVPWSG) showing that the
transpose graph reacts very peculiarly to compression after
lexicographical or Gray-code sorting.

* A [paper](http://vigna.di.unimi.it/papers.php#BRVH) about
[HyperBall](http://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/algo/HyperBall.html)
(then named HyperANF), our tool for computing an approximation of the
neighbourhood function, reachable nodes and geometric centralities of
massive graphs. More information can be found in this
[preprint](http://vigna.di.unimi.it/papers.php#BoVHB).

* [HyperBall](docs/it/unimi/dsi/webgraph/algo/HyperBall.html) was used to
find out that on average there are just [four degrees of
separation](http://vigna.di.unimi.it/papers.php#BBRFDS) on
[Facebook](http://facebook.com/), and the experiment was reported by the
[New York
Times](http://nytimes.com/2011/11/22/technology/between-you-and-me-4-74-degrees.html).
Alas, the degrees were actually 3.74 (one less than the [average
distance](http://law.di.unimi.it/webdata/fb-current/)), but the off-by-one
between graph theory (“distance”) and sociology (“degrees of separation”)
generated a lot of confusion.

* A [paper](http://vigna.di.unimi.it/papers.php#BPVULCRAGC) were we
describe our efforts to compress one of the largest social graphs
available—the graph of commits of the Software Heritage archive.

* A [paper](http://vigna.di.unimi.it/papers.php#FVZWNG) about our effort
to bring WebGraph to [Rust](https://www.rust-lang.org/).

## Acknowledgments

This software has been partially supported by project SERICS (PE00000014) under
the NRRP MUR program funded by the EU - NGEU, and by project ANR COREGRAPHIE,
grant ANR-20-CE23-0002 of the French Agence Nationale de la Recherche. Views and
opinions expressed are however those of the authors only and do not necessarily
reflect those of the European Union or the Italian MUR. Neither the European
Union nor the Italian MUR can be held responsible for them.

[WebGraph framework]: https://webgraph.di.unimi.it/
[`webgraph`]: https://crates.io/crates/webgraph
[webgraph repo]: /webgraph
[`webgraph-algo`]: https://crates.io/crates/webgraph-algo
[algo repo]: /algo
[`webgraph-cli`]: https://crates.io/crates/webgraph-cli
[cli repo]: /cli
