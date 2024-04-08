#!/usr/bin/env bash
# This script runs the entire pipeline to compress a graph using LLP
# The first argument is the basename of the graph to compress
set -e

GRAPH="$1" # the graph basename is the first argument
WEBGRAPH="cargo run --release --"

# Step 1: Create the Elias Fano
$WEBGRAPH build ef $GRAPH
# Step2: Run a BFS traversal to get the initial permutation
$WEBGRAPH bfs $GRAPH $GRAPH.bfs
# Step3: Create a simplified view of the graph with the BFS permutation
$WEBGRAPH simplify $GRAPH $GRAPH-simplified --permutation $GRAPH.bfs
# Step4: Create the Elias Fano for the simplified graph
$WEBGRAPH build ef $GRAPH-simplified
# Step4: Create the Degrees Cumulative Function
$WEBGRAPH build dcf $GRAPH-simplified
# Step5: Run LLP to get the final permutation
$WEBGRAPH llp $GRAPH-simplified $GRAPH.llp
# Step6: Merge the two permutations
$WEBGRAPH merge-perms $GRAPH.merged_perm $GRAPH.bfs $GRAPH.llp
# Step7: Apply both permutations to the original graph
$WEBGRAPH recompress $GRAPH $GRAPH-final --permutation $GRAPH.merged_perm
# Step8: Create the final Elias Fano
$WEBGRAPH build ef $GRAPH
