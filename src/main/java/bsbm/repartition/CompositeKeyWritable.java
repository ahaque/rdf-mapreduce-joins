/**
 * @author Albert Haque
 * May 2014
 */

package bsbm.repartition;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
 
public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {
 
	private String value;
	private int tag;
 
	public CompositeKeyWritable() {
	}
 
	public CompositeKeyWritable(String value, int t) {
		this.value = value;
		this.tag = t;
	}
	
	public CompositeKeyWritable(byte[] value, int t) {
		this.value = new String(value);
		this.tag = t;
	}
 
	@Override
	public String toString() {
		return value;
	}
 
	public void readFields(DataInput dataInput) throws IOException {
		value = WritableUtils.readString(dataInput);
		tag = WritableUtils.readVInt(dataInput);
	}
 
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, value);
		WritableUtils.writeVInt(dataOutput, tag);
	}
 
	public int compareTo(CompositeKeyWritable objKeyPair) {
		int result = value.compareTo(objKeyPair.value);
		if (0 == result) {
			result = Double.compare(tag, objKeyPair.tag);
		}
		return result;
	}
 
	public String getValue() {
		return value;
	}
	
	public Text getText() {
		return new Text(value);
	}
 
	public void setValue(String value) {
		this.value = value;
	}
 
	public int getTag() {
		return tag;
	}
 
	public void setTag(int t) {
		this.tag = t;
	}
}