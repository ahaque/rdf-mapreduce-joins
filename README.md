MapReduce Join Algorithms for RDF
=========
I implemented two join algorithms: the reduce side join and the improved repartition join. This is a work in progress.

Reduce Side Join
---------
The reduce side join is a join algorithm for use in MapReduce environments (e.g. Hadoop).
Each mapper node reads its local data blocks and extracts the join attribute for that record.
These records are then sent to the appropriate reducer and the actual comparison is done at the reducer nodes.

Repartition Join
---------
The repartition join uses a compound key to identify which relation the row originates from. It uses a custom Hadoop Partitioner, Sort, and Grouping function.

Software
---------
This join is implemented using Hadoop 1.2.1 and HBase 0.94.17.