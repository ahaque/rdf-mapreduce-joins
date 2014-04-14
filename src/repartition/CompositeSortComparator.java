package repartition;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeSortComparator extends WritableComparator {
	 
	protected CompositeSortComparator() {
		super(CompositeKeyWritable.class, true);
	}
 
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		// Sort on all attributes of composite key
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
 
		int compareResult = key1.getValue().compareTo(key2.getValue());
		// If same join key
		if (compareResult == 0) {
			return Double.compare(key1.getTag(), key2.getTag());
		}
		return compareResult;
	}
}