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
Our second, more advanced approach was inspired by Luby's probabilistic algorithm for maximal matching. This algorithm works iteratively, randomly assigning priorities to edges in each round. Every vertex then picks the incident edge with the highest priority that is the edge with the largest random value. When two connected vertices mutually choose the same edge as their top choice, that edge gets added to the matching. Afterward, matched vertices are marked and excluded from future rounds to ensure there are no overlapping matches. The process continues until no new matches can be formed or a predetermined iteration limit is reached. Since all decisions are based solely on local information, the algorithm is scalable and efficiently handles large graphs through parallel processing.

We implemented this using Spark's aggregateMessages API to quickly find the highest-priority edges for each vertex. To keep memory usage low, we broadcast the set of already matched vertices and filtered them out in subsequent iterations. This design avoids the global synchronization and memory issues seen in simpler greedy algorithms, particularly when working with dense or complex graphs.

Our experiments showed that Luby’s algorithm is highly effective on large-scale datasets, such as com-orkut.ungraph.csv, twitter_original_edges.csv, and soc-LiveJournal1.csv. It typically produced matchings with over 900,000 edges in fewer than 10 iterations, even for graphs with more than 117 million edges, completing these runs in under 500 seconds due to its parallel and efficient structure. On smaller datasets like musae_ENGB_edges.csv and log_normal_100.csv, Luby’s method still performed accurately, though the simpler GreedyMaxMatching often ran faster because it had less computational overhead.

The dataset soc-pokec-relationships.csv particularly highlighted this trade-off: when optimized by increasing partitioning, GreedyMaxMatching yielded significantly larger matchings (740,633 edges) compared to Luby’s algorithm (595,834 edges). These results indicate that while Luby’s approach is robust and scalable for general-purpose use on massive graphs, the optimized greedy method can sometimes outperform it in targeted scenarios such as core-periphery structures like soc-pock as its based on a social media. Ultimately, Luby’s method consistently provides an effective and scalable solution for matching in large, real-world networks.


## Link to Solutions:

Click on file_solutions not _MACOSX (also don't know what .DS_Store is but that is not one of the solutions): https://drive.google.com/file/d/1BqgNJM_tSxLh-dXeyk_8G58gk4ycb0l0/view?usp=share_link


## Link to 5/2 Slideshow: 

https://docs.google.com/presentation/d/1Ik2CgcAqT0gtpXBfQG9XTDm2ihWtSrsJrG6OwSOMZTU/edit#slide=id.p

