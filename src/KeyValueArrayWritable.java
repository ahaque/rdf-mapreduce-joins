import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.ArrayWritable;

public class KeyValueArrayWritable extends ArrayWritable {

    public KeyValueArrayWritable() {
      super(KeyValue.class);
    }

    public KeyValueArrayWritable(KeyValue[] writables) {
      super(KeyValue.class, writables);
    }
  }