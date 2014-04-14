package repartition;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeGroupingComparator extends WritableComparator {
	protected CompositeGroupingComparator() {
		super(CompositeKeyWritable.class, true);
	}
 
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
		return key1.getValue().compareTo(key2.getValue());
	}
}