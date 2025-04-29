# Final Project: Matching

Names: Alexander Sharpe and Cristobal Benavides

## Results Table

|           File name           |        Number of edges       | Size of Matching | Run Time (s)| Configuration (cores,_) | Algorithm
| ------------------------------| ---------------------------- | --- | ----| ---| --- |
| com-orkut.ungraph.csv         | 117185083                    | | | |  |
| twitter_original_edges.csv    | 63555749                     | | | | |
| soc-LiveJournal1.csv          | 42851237                     | | | | |
| soc-pokec-relationships.csv   | 22301964                     | 1486549 | 91 | 2x2, 200 partitions| GreedyMaxMatch|
| musae_ENGB_edges.csv          | 35324                        | 2954 | 2 | 2x2, Default partitions | GreedyMaxMatch |
| log_normal_100.csv            | 2671                         | 50 | 1 | 2x2, Default partitions | GreedyMaxMatch|

** Output files can be found in file_solutions.zip (all output files passed the verifier) 
  
## Approach 1: 

The first approach we used was a fairly simple and greedy algorithm. This algorithm has two phases. The first phase consists of a local greedy pass during which each partition (executor) scans its edges and chooses a conflict free matching subset. The second phase searches for conflicts between the partitions by selecting only edge per vertex for the matching and removing any duplicates. This approach allows for scalability as it partitions the input file, and then sends each partition to a separate executor that creates a small subset of the total matching. These subsets are then combined to create the final matching. Additionally, one can increase the number of partitions manually for larger files by editing the line "sc.textFile(input_path, numPartitions)." While it is quite simple and perhaps not the most accurate in every case, it is very fast for smaller files with default partitions and very fast for larger files with added partitions. As shown by the results above, it was quite accurate for some cases: we know that the perfect solution to the matching problem is upper bounded by $$\lfloor \frac{|V|}{2} \rfloor$$. For log_normal_100.csv, there are only 100 vertices, so the best possible matching is 50, which we obtained with our algorithm. musae_ENGB_edges.csv has approximately 7000 vertices, so our answer of 2954 is fairly good with the default partitions. In general though, we see a drop in performance when the graphs are sparser or when the graphs are not bipartite due to the greedy nature of the algorithm (it just picks the first valid edge even though there may be more optimal choices). Compared to a more complex algorithm like Luby's, this algorithm takes 1 round versus $$O(\log(n))$$ rounds. This is useful if it is more expensive to do multiple rounds, however it will become less useful on massive graphs where the completion time is already minutes long. In terms of the optimality of the matching, both the greedy and Luby guarantee 1/2-approximation to an optimal maximum matching, although Luby will generally perfom better due to avoiding partiton bias. The desire for a slightly more accurate and scalable algorithm led us to the next algorithm as described below.

- Technical Details: 
- Scalability:
- Theoretical Merits:
- Novelty?:

## Approach 2: 

## Link to 5/2 Slideshow: 

https://docs.google.com/presentation/d/1Ik2CgcAqT0gtpXBfQG9XTDm2ihWtSrsJrG6OwSOMZTU/edit#slide=id.p

