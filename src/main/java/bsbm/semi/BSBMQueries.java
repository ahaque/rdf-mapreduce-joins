package bsbm.semi;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author David Alves
 */
public class BSBMQueries {

  public static class KeyValuArrayWritable extends ArrayWritable {

    public KeyValuArrayWritable() {
      super(KeyValue.class);
    }

    public KeyValuArrayWritable(KeyValue[] writables) {
      super(KeyValue.class, writables);
    }
  }

  public static class JoinReducer extends Reducer<Text, KeyValuArrayWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<BSBMQueries.KeyValuArrayWritable> values, Context context) throws IOException, InterruptedException {
      StringBuilder builder = new StringBuilder();
      for (BSBMQueries.KeyValuArrayWritable array : values) {
        for (KeyValue kv : (KeyValue[]) array.toArray()) {
          builder.append(new String(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength()))
            .append(" : ")
            .append(new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength()))
            .append("\n");
        }
      }
      context.write(key, new Text(builder.toString()));
    }
  }
}
