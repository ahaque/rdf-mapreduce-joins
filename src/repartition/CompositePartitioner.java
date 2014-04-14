package repartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
 
public class CompositePartitioner extends Partitioner<CompositeKeyWritable, Text> {
 
	@Override
	public int getPartition(CompositeKeyWritable key, Text value, int numReduceTasks) {
		return (key.getValue().hashCode() % numReduceTasks);
	}
}