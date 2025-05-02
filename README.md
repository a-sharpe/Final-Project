# Final Project: Matching

Names: Alexander Sharpe and Cristobal Benavides

## Results Table

|           File name           |        Number of edges       | Size of Matching | Run Time (s)| Configuration (cores,_) | Iterations | Algorithm |
| ------------------------------| ---------------------------- | ---------------- | ------------| ------------------------| ---------- | ----------|
| com-orkut.ungraph.csv         | 117185083                    | 1336541 | 483 | 3x4 on Balanced Persistent Disk | 9 | Luby |
| twitter_original_edges.csv    | 63555749                     | 92237  | 257  | 3x4 on Standard Disk | 8 | Luby|
| soc-LiveJournal1.csv          | 42851237                     | 1571286 |490  | 3x4 on Standard Disk | 9 | Luby |
| soc-pokec-relationships.csv   | 22301964                     | 595834 | 439 | 2x2 | 7 | Luby|
| soc-pokec-relationships.csv   | 22301964                     | 740633 | 337 | 2x2, 200 partitions| 2 | GreedyMaxMatch|
| musae_ENGB_edges.csv          | 35324                        | 2259 | 62 | 2x2 | 5 | Luby |
| musae_ENGB_edges.csv          | 35324                        | 2749 | 62 | 2x2, Default partitions | 2 | GreedyMaxMatch |
| log_normal_100.csv            | 2671                         | 50 | 33 | 2x2, Default partitions | 2 |  GreedyMaxMatch|

** Output files can be found in file_solutions.zip (all output files passed the verifier) 

** Note: the results above are all from GCP, but in some cases the local results were much faster
  
## Approach 1: 

The first approach we used was a modified and more scalable greedy algorithm. This algorithm has two phases. The first phase consists of establishing a priority hierarchy by finding the degree of each vertex, then creating and broadcasting a map with the prioritized vertices. The vertices with the lowest priority (degree) will be matched first in order to help with the sparse corners issue that is found within the matching problem. The second phase is an iterative peel loop, where edges that touch matched vertices are removed, then a greedy loop is run to sort (by priority), collect, and add unmatched edges. This process repeats until no new matches appear. This algorithm demonstrates a marked improvement over a regular greedy algorithm because of the novel priority system it implements, which allows more optimal matchings to be made as opposed to just choosing the first unmatched edge. This approach allows for scalability as it partitions the input file during preprocessing, and then sends each partition to a separate executor. Additionally, one can increase the number of partitions manually for larger files by editing the line "sc.textFile(input_path, numPartitions)," which is very helpful when dealing with larger files. Another aspect of scalability is the fact that this algorithm uses broadcasting to reduce the memory overhead, which is unlike the baseline greedy matching algorithm. While it somewhat simple, it is very fast for smaller files with default partitions and very fast for larger files with added partitions. As shown by the results above, it was quite accurate for some cases: we know that the perfect solution to the matching problem is upper bounded by $$\frac{|V|}{2}$$. For log_normal_100.csv, there are only 100 vertices, so the best possible matching is 50, which we obtained with our algorithm. musae_ENGB_edges.csv has approximately 7000 vertices, so our answer of 2749 is fairly good with the default partitions. In general though, we see a drop in performance when the graphs are very dense or when the graphs are not bipartite due to the greedy part of the algorithm. Additionally, due to the greediness of the algorithm, it begins to run into OOM errors in the larger file, which led us to develop a more complex and scalable algorithm inspired by Luby.

## Approach 2: 
Our second, more advanced approach was inspired by Luby's probabilistic algorithm for maximal matching. The algorithm operates iteratively, assigning random priorities to edges in each round. Each vertex selects the incident edge with the highest priority (the one with the largest random value). If two connected vertices mutually select the same edge, that edge is included in the matching. Once an edge is matched, the involved vertices are removed from further consideration to prevent overlaps. This iterative process continues until no new matches can be formed or a predefined iteration limit is reached. Since decisions rely only on local information, the algorithm scales naturally and efficiently handles massive graphs by parallelizing computations across multiple cores and machines.

To implement this efficiently, we leveraged Spark’s aggregateMessages API to rapidly determine the highest-priority edges for each vertex. Additionally, we broadcast the set of already matched vertices in each round to maintain memory efficiency and filter them from subsequent iterations. This design minimizes global synchronization and mitigates memory bottlenecks that typically affect simpler greedy algorithms, especially on dense or non-bipartite graphs.

Our experiments confirmed Luby’s algorithm as effective for large-scale datasets like com-orkut.ungraph.csv (over 11 million edges), twitter_original_edges.csv (over 6 million edges), and soc-LiveJournal1.csv (over 4 million edges), consistently delivering large matchings in under 10 iterations and less than 500 seconds, showcasing its scalability and parallel efficiency.

For smaller datasets, such as musae_ENGB_edges.csv and log_normal_100.csv, Luby’s algorithm remained accurate but was outperformed in speed by our simpler GreedyMaxMatching algorithm due to lower computational overhead. Specifically, the soc-pokec-relationships.csv dataset highlighted this clearly: GreedyMaxMatching (with optimized partitioning) achieved a significantly larger matching (740,633 edges) compared to Luby's method (595,834 edges). This demonstrates that while Luby’s method is robust and generally scalable for massive graphs, an optimized greedy approach can outperform it in specific, well-partitioned scenarios.

## Link to Solutions:

Click on file_solutions not _MACOSX (also don't know what .DS_Store is but that is not one of the solutions): https://drive.google.com/file/d/1BqgNJM_tSxLh-dXeyk_8G58gk4ycb0l0/view?usp=share_link


## Link to 5/2 Slideshow: 

https://docs.google.com/presentation/d/1Ik2CgcAqT0gtpXBfQG9XTDm2ihWtSrsJrG6OwSOMZTU/edit#slide=id.p

