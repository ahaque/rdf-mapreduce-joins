import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class KeyValueArrayWritable extends ArrayWritable {

    @SuppressWarnings("unchecked")
	public KeyValueArrayWritable() {
      super((Class<? extends Writable>) KeyValue.class);
    }

    @SuppressWarnings("unchecked")
	public KeyValueArrayWritable(KeyValue[] writables) {
      super((Class<? extends Writable>) KeyValue.class, (Writable[]) writables);
    }
  }