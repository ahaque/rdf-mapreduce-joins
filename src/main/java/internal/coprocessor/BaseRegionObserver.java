package internal.coprocessor;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.CompoundBloomFilter;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;

/**
 * @author David Alves
 */
public class BaseRegionObserver extends org.apache.hadoop.hbase.coprocessor.BaseRegionObserver {

  protected static final Object TABLE_INDEXES_LOCK = new Object();
  protected static volatile Map<String, NavigableMap<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>>> TABLE_INDEXES
    = Maps.newHashMap();

  public static void printHFileMatch(BloomFilter bf, byte[] buf, int offset, int length) {
    try {
      byte[] key = new byte[length];
      System.arraycopy(buf, offset, key, 0, length);
      CompoundBloomFilter filter = (CompoundBloomFilter) bf;
      Field readerField = CompoundBloomFilter.class.getDeclaredField("reader");
      Field indexField = CompoundBloomFilter.class.getDeclaredField("index");
      readerField.setAccessible(true);
      indexField.setAccessible(true);
      HFileBlockIndex.BlockIndexReader index = (HFileBlockIndex.BlockIndexReader) indexField.get(filter);
      HFile.Reader reader = (HFile.Reader) readerField.get(filter);
      System.out.println("Key " + new String(key) + " Keys[ " + new String(reader.getFirstRowKey()) + " : " +
        new String(reader.getLastRowKey()) + "] Index: pos: " + index.rootBlockContainingKey(buf, offset, length) +
        " BF match: " + filter.contains(buf, offset, length, null));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
