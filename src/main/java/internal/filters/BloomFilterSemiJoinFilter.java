package internal.filters;


import internal.util.GlobalBloomFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompoundBloomFilterBase;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A filter that performs a Bloom Filter semi join on records.
 * Client sets the relevant column families (or leaves them as null for "all").
 * A Co-Processor in HBase actually sets the bloom filters that are required for this to work.
 * This either checks for the existence of the following entries in the global bloom filter:
 * - COL_NAME tests if the column name exists as a key somewhere (can only be used with ROW Bloom Filters)
 * - COL_VALUE tests if the column value exists as a key somewhere (can only be used with ROW Bloom Filters)
 * - COL_NAME_ROW tests if the reverse relation row + col_name exists as somewhere else
 * <p/>
 * This filter should be the trailing filter as it is potentially heavyweight (as in reads from HDFS), i.e. this should
 * be only used if the row passed all other filters (however this is not enforced).
 * <p/>
 * Note: this filter is an exclusion filter, i.e. it says nothing about whether a KeyValue should be included,
 * only if that particular KeyValue should be excluded.
 */
public class BloomFilterSemiJoinFilter extends FilterBase {

  public interface BloomFilterLookupKeyBuilder extends Writable {

    public boolean isRowKeyOnly();

    public byte[] bloomFilterKey(byte[] buffer, int offset, int length);

    public byte[] bloomFilterKey(KeyValue kv);

  }

  /**
   * Helper for BloomFilterLookupKeyBuilder that doesn't force to implement write() and readFields(), in the case
   * the key builder is stateless;
   */
  public static abstract class BloomFilterLookupKeyBuilderBase implements BloomFilterLookupKeyBuilder {

    protected static final CompoundBloomFilterBase BF_ENTRY_CREATOR = new CompoundBloomFilterBase();

    @Override
    public boolean isRowKeyOnly() {
      return false;
    }

    @Override
    public byte[] bloomFilterKey(byte[] buffer, int offset, int length) {
      return new byte[0];
    }

    @Override
    public byte[] bloomFilterKey(KeyValue kv) {
      return new byte[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    }

  }

  /**
   * Default BF lookup key builder thar just inverts the relation (i.e. given a ROW COL looks up COL ROW)
   */
  public static class DefaultBloomFilterLookupKeyBuilder extends BloomFilterLookupKeyBuilderBase {

    @Override
    public byte[] bloomFilterKey(KeyValue kv) {
      return BF_ENTRY_CREATOR.createBloomKey(kv.getBuffer(), kv.getQualifierOffset(),
        kv.getQualifierLength(), kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
    }
  }

  /**
   * Tests if the provided KeyValue's Qualifier, concatenated with a statically defined property,
   * exists in any bloomfilter.
   */
  public static class StaticColumnBloomFilterLookupKeyBuilder extends BloomFilterLookupKeyBuilderBase {

    private byte[] staticColumn;

    public StaticColumnBloomFilterLookupKeyBuilder() {}

    public StaticColumnBloomFilterLookupKeyBuilder(byte[] staticColumn) {
      this.staticColumn = checkNotNull(staticColumn);
    }

    @Override
    public byte[] bloomFilterKey(KeyValue kv) {
      return BF_ENTRY_CREATOR.createBloomKey(kv.getBuffer(), kv.getQualifierOffset(),
        kv.getQualifierLength(), staticColumn, 0, staticColumn.length);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      staticColumn = Bytes.readByteArray(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      Bytes.writeByteArray(dataOutput, staticColumn);
    }

  }

  /**
   * Row Key BF Key builders like this one are used early on and can be used to exclude further scanning into the row.
   */
  public static class StaticRowBloomFilterLookupKeyBuilder extends BloomFilterLookupKeyBuilderBase {

    private byte[] staticColumn;

    public StaticRowBloomFilterLookupKeyBuilder() {}

    public StaticRowBloomFilterLookupKeyBuilder(byte[] staticColumn) {
      this.staticColumn = checkNotNull(staticColumn);
    }

    @Override
    public boolean isRowKeyOnly() {
      return true;
    }

    @Override
    public byte[] bloomFilterKey(byte[] buffer, int offset, int length) {
      return BF_ENTRY_CREATOR.createBloomKey(buffer, offset,
        length, staticColumn, 0, staticColumn.length);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      staticColumn = Bytes.readByteArray(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      Bytes.writeByteArray(dataOutput, staticColumn);
    }

  }

  private static final Log LOG = LogFactory.getLog(BloomFilterSemiJoinFilter.class);


  public enum SemiJoinType {
    /**
     * Semi-join on COL_NAME as the key.
     * I.e. test if COL_NAME exists as Row key somewhere.
     */
    COL_NAME,
    /**
     * Semi-join on COL_VALUE as the key.
     * I.e. test if COL_VALUE exists as Row key somewhere.
     */
    COL_VALUE,
    /**
     * Semi-join on COL_NAME+ROW as the key.
     * I.e. test if the pair ROW = COL_NAME and COL_NAME = ROW
     */
    COL_NAME_ROW;
  }

  /**
   * The type of semi-join to implement
   */
  private SemiJoinType semiJoinType;
  /**
   * The column family to which this semi-join filter will be applied
   */
  private byte[] columnFamily;
  /**
   * The join key filter, usually a COL_NAME or COL_VALUE filter that indicates whether the semi-join
   * should be applied to a specific KeyValue, this filter does NOT exclude values from the scan
   * i.e. even if it returns something other than ReturnCode.INCLUDE, the key value won't be excluded
   * based solely on the joinKeyFilter output.
   */
  private Filter joinKeyFilter;

  /**
   * Set server side.
   */
  private GlobalBloomFilter filter;

  BloomFilterLookupKeyBuilder keyBuilder = new DefaultBloomFilterLookupKeyBuilder();

  public BloomFilterSemiJoinFilter() {
  }

  public BloomFilterSemiJoinFilter(SemiJoinType semiJoinType, byte[] columnFamily, Filter joinKeyFilter) {
    this.semiJoinType = checkNotNull(semiJoinType);
    this.columnFamily = checkNotNull(columnFamily);
    this.joinKeyFilter = checkNotNull(joinKeyFilter);
  }

  public BloomFilterSemiJoinFilter(SemiJoinType semiJoinType, byte[] columnFamily, Filter joinKeyFilter,
    BloomFilterLookupKeyBuilder keyBuilder) {
    this.semiJoinType = checkNotNull(semiJoinType);
    this.columnFamily = checkNotNull(columnFamily);
    this.joinKeyFilter = checkNotNull(joinKeyFilter);
    this.keyBuilder = checkNotNull(keyBuilder);
  }

  public BloomFilterSemiJoinFilter(SemiJoinType semiJoinType, byte[] columnFamily,
    BloomFilterLookupKeyBuilder keyBuilder) {
    this.semiJoinType = checkNotNull(semiJoinType);
    this.columnFamily = checkNotNull(columnFamily);
    this.keyBuilder = checkNotNull(keyBuilder);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(semiJoinType.ordinal());
    dataOutput.writeInt(columnFamily.length);
    dataOutput.write(columnFamily);
    if (joinKeyFilter != null) {
      dataOutput.writeBoolean(true);
      dataOutput.writeUTF(this.joinKeyFilter.getClass().getName());
      this.joinKeyFilter.write(dataOutput);
    } else {
      dataOutput.writeBoolean(false);
    }
    // when not using the default BF key lookup builder serialize it
    if (keyBuilder.getClass() != DefaultBloomFilterLookupKeyBuilder.class) {
      dataOutput.writeBoolean(true);
      dataOutput.writeUTF(this.keyBuilder.getClass().getName());
      this.keyBuilder.write(dataOutput);
    } else {
      dataOutput.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.semiJoinType = SemiJoinType.values()[dataInput.readInt()];
    this.columnFamily = new byte[dataInput.readInt()];
    dataInput.readFully(this.columnFamily);
    if (dataInput.readBoolean()) {
      String joinKeyFilterClass = dataInput.readUTF();
      try {
        this.joinKeyFilter = (Filter) Class.forName(joinKeyFilterClass).newInstance();
      } catch (Exception e) {
        LOG.warn("Could not instantiate joinKeyFilter class: " + joinKeyFilterClass, e);
        throw new IOException(e);
      }
      this.joinKeyFilter.readFields(dataInput);
    }
    if (dataInput.readBoolean()) {
      String keyBuilderClass = dataInput.readUTF();
      try {
        this.keyBuilder = (BloomFilterLookupKeyBuilder) Class.forName(keyBuilderClass).newInstance();
      } catch (Exception e) {
        LOG.warn("Could not instantiate keyBuilder class: " + keyBuilderClass, e);
        throw new IOException(e);
      }
      this.keyBuilder.readFields(dataInput);
    }
  }

  public SemiJoinType getSemiJoinType() {
    return semiJoinType;
  }

  public byte[] getColumnFamily() {
    return columnFamily;
  }

  public void setFilter(GlobalBloomFilter filter) {
    this.filter = filter;
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    if (keyBuilder.isRowKeyOnly()) {
      byte[] bfEntry = keyBuilder.bloomFilterKey(buffer, offset, length);
      ReturnCode result = includeIfContains(columnFamily, 0, columnFamily.length, buffer, offset, length, bfEntry, 0, bfEntry.length);
      if (result != ReturnCode.INCLUDE) {
        return true;
      }
      //      LOG.info("Testing: " + new String(bfEntry) + " got: " + result);
    }
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {


    // if the filter is not set return INCLUDE
    if (filter == null) {
      return ReturnCode.INCLUDE;
    }

    // if the key builder only needs the Row Key return include
    if (keyBuilder.isRowKeyOnly()) {
      return ReturnCode.INCLUDE;
    }

    // If we're looking for a join key and this KV is not the one return INCLUDE
    if (joinKeyFilter != null) {
      ReturnCode innerFilterResult = joinKeyFilter.filterKeyValue(kv);
      if (innerFilterResult != ReturnCode.INCLUDE) {
        return ReturnCode.INCLUDE;
      }
    }

    ReturnCode result;
    switch (semiJoinType) {
      case COL_NAME:
        result = includeIfContains(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
          // the qualifier will we the row key we search for
          kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
          // the quqalifier is complete BF match we're looking for
          kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
        break;
      case COL_VALUE:
        result = includeIfContains(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
          // the value will we the row key we search for
          kv.getBuffer(), kv.getValueOffset(), kv.getValueLength(),
          // the value is complete BF match we're looking for
          kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
        break;
      case COL_NAME_ROW:
        // need to concatenate COL_NAME + ROW_KEY to test
        byte[] colNameRowKey = keyBuilder.bloomFilterKey(kv);
        result = includeIfContains(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
          // the qualifier will we the row key we search for
          kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
          // the qualifier+the row key will be the BF match we're looking for
          colNameRowKey, 0, colNameRowKey.length);
        break;
      default:
        throw new IllegalStateException();
    }
    return result;
  }

  private ReturnCode includeIfContains(byte[] cf, int cfOffeset, int cfLength,
    byte[] key, int keyOffset, int keyLength,
    byte[] bytes, int offset, int length) {
    return filter.contains(cf, cfOffeset, cfLength, key, keyOffset, keyLength, bytes, offset, length) ?
      // if the BF matches we include the row so far (other filters may still exclude it), if not we skip the row
      ReturnCode.INCLUDE : ReturnCode.NEXT_ROW;
  }

}
