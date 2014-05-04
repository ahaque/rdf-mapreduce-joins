package bsbm.repartition;

import org.apache.hadoop.mapreduce.Partitioner;
import bsbm.sortmerge.KeyValueArrayWritable;
 
public class CompositePartitioner extends Partitioner<CompositeKeyWritable, KeyValueArrayWritable> {
 
	@Override
	public int getPartition(CompositeKeyWritable key, KeyValueArrayWritable value, int numReduceTasks) {
		return (Math.abs(key.getValue().hashCode()) % numReduceTasks);
	}
}