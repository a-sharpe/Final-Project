# Final Project: Matching

Names: Alexander Sharpe and Cristobal Benavides

## Results Table

|           File name           |        Number of edges       | Size of Matching | Run Time (s)| Core Configuration | Algorithm
| ------------------------------| ---------------------------- | --- | ----| ---| --- |
| com-orkut.ungraph.csv         | 117185083                    | | | |  |
| twitter_original_edges.csv    | 63555749                     | | | | |
| soc-LiveJournal1.csv          | 42851237                     | | | | |
| soc-pokec-relationships.csv   | 22301964                     | | | | |
| musae_ENGB_edges.csv          | 35324                        | 2954 | 2 | 2x2 | GreedyMaxMatch |
| log_normal_100.csv            | 2671                         | 50 | 1 | 2x2 | GreedyMaxMatch|

** Output files can be found in file_solutions.zip (all output files passed the verifier) 
  
## Approach 1: 

The first approach we used was a fairly simple and greedy algorithm. This algorithm has two phases. The first phase consists of a local greedy pass during which each partition (executor) scans its edges and chooses a conflict free matching subset. The second phase searches for conflicts between the partitions by selecting only edge per vertex for the matching and removing any duplicates. This approach allows for scalability as it partitions the input file, and then sends each partition to a separate executor that creates a small subset of the total matching. These subsets are then combined to create the final matching. Additionally, one can increase the number of partitions manually for larger files by editing the line "sc.textFile(input_path, numPartitions)." While it is quite simple, it is also extremely fast for smaller files and as shown by the results above, was quite accurate: we know that the perfect solution to the matching problem is upper bounded by $$\cap $$ 

- Technical Details: 
- Scalability:
- Theoretical Merits:
- Novelty?:

## Approach 2: 

## Link to 5/2 Slideshow: 

https://docs.google.com/presentation/d/1Ik2CgcAqT0gtpXBfQG9XTDm2ihWtSrsJrG6OwSOMZTU/edit#slide=id.p

