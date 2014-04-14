package repartition;

import org.apache.hadoop.mapreduce.Partitioner;
import sortmerge.KeyValueArrayWritable;
 
public class CompositePartitioner extends Partitioner<CompositeKeyWritable, KeyValueArrayWritable> {
 
	@Override
	public int getPartition(CompositeKeyWritable key, KeyValueArrayWritable value, int numReduceTasks) {
		return (key.getValue().hashCode() % numReduceTasks);
	}
}