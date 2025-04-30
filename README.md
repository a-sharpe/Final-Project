# Final Project: Matching

Names: Alexander Sharpe and Cristobal Benavides

## Results Table

|           File name           |        Number of edges       | Size of Matching | Run Time (s)| Configuration (cores,_) | Iterations | Algorithm
| ------------------------------| ---------------------------- | --- | ----| ---| --- | --|
| com-orkut.ungraph.csv         | 117185083                    | | | |  | |
| twitter_original_edges.csv    | 63555749                     | 92237 -BAD | 257| 3x4 on Standard Disk | 8 | Luby|
| soc-LiveJournal1.csv          | 42851237                     | 1571286 |490  | 3x4 on Standard Disk | 9 | Luby |
| soc-pokec-relationships.csv   | 22301964                     | 595871 | 414 | 2x2 | 7 | Luby|
| soc-pokec-relationships.csv   | 22301964                     | 740633 | 337 | 2x2, 200 partitions| 2 | GreedyMaxMatch|
| musae_ENGB_edges.csv          | 35324                        | 2259 | 62 | 2x2 | 5 | Luby |
| musae_ENGB_edges.csv          | 35324                        | 2749 | 62 | 2x2, Default partitions | 2 | GreedyMaxMatch |
| log_normal_100.csv            | 2671                         | 50 | 33 | 2x2, Default partitions | 2 |  GreedyMaxMatch|

** Output files can be found in file_solutions.zip (all output files passed the verifier) 

** Note: the results above are all from GCP, but in many cases the local results were much faster
  
## Approach 1: 

The first approach we used was a modified and more scalable greedy algorithm. This algorithm has two phases. The first phase consists of establishing a priority hierarchy by finding the degree of each vertex, then creating and broadcasting a map with the prioritized vertices. The vertices with the lowest priority (degree) will be matched first in order to help with the sparse corners issue that is found within the matching problem. The second phase is an iterative peel loop, where edges that touch matched vertices are removed, then a greedy loop is run to sort (by priority), collect, and add unmatched edges. This process repeats until no new matches appear. This algorithm demonstrates a marked improvement over a regular greedy algorithm because of the novel priority system it implements, which allows more optimal matchings to be made as opposed to just choosing the first unmatched edge. This approach allows for scalability as it partitions the input file during preprocessing, and then sends each partition to a separate executor. Additionally, one can increase the number of partitions manually for larger files by editing the line "sc.textFile(input_path, numPartitions)," which is very helpful when dealing with larger files. While it somewhat simple, it is very fast for smaller files with default partitions and very fast for larger files with added partitions. As shown by the results above, it was quite accurate for some cases: we know that the perfect solution to the matching problem is upper bounded by $$\frac{|V|}{2}$$. For log_normal_100.csv, there are only 100 vertices, so the best possible matching is 50, which we obtained with our algorithm. musae_ENGB_edges.csv has approximately 7000 vertices, so our answer of ______ is fairly good with the default partitions. In general though, we see a drop in performance when the graphs are very dense or when the graphs are not bipartite due to the greedy part of the algorithm. 


- Technical Details: 
- Scalability:
- Theoretical Merits:
- Novelty?:

## Approach 2: 

## Link to 5/2 Slideshow: 

https://docs.google.com/presentation/d/1Ik2CgcAqT0gtpXBfQG9XTDm2ihWtSrsJrG6OwSOMZTU/edit#slide=id.p

