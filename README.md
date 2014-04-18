MapReduce Join Algorithms for RDF
=========
I implemented two join algorithms: the sort-merge join and the improved repartition join. I am in the process of evaluating the
algorithms on Amazon EC2 and Elastic MapReduce. A lot of work is required to generate, store, and transfer the terabyte size
files on Amazon S3 - all while trying to keep costs low.

Sort-Merge Join
---------
The sort-merge (reduce side) join is a join algorithm for use in MapReduce environments (e.g. Hadoop).
Each mapper node reads its local data blocks and extracts the join attribute for that record.
These records are then sent to the appropriate reducer and the actual comparison is done at the reducer nodes.

Repartition Join
---------
The repartition join uses a compound key to identify which relation the row originates from.
It uses a custom Hadoop Partitioner, Sort, and Grouping function. You can read a paper detailing this implementation here:

[1] Blanas, Spyros, et al. "A comparison of join algorithms for log processing in mapreduce."
Proceedings of the 2010 ACM SIGMOD International Conference on Management of data. ACM, 2010.

Software
---------
This join is implemented using Hadoop 2.2.0 and HBase 0.94.7.