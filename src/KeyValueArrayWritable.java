import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class KeyValueArrayWritable extends ArrayWritable {

    public KeyValueArrayWritable() {
      super(KeyValue.class);
    }

    public KeyValueArrayWritable(KeyValue[] writables) {
      super(KeyValue.class, writables);
    }
  }