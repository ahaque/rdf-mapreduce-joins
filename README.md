Reduce Side Join
=========

The reduce side join is a join algorith for use in MapReduce environments (e.g. Hadoop). Each mapper node reads its local data blocks and extracts the join attribute for that record. These records are then sent to the appropriate reducer and the actual comparison is done at the reducer nodes. Hence the name Reduce Side join.