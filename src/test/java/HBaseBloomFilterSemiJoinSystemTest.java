/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import internal.coprocessor.NonAggregatingRegionObserver;
import internal.filters.BloomFilterSemiJoinFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.CompoundBloomFilterBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

import static internal.filters.BloomFilterSemiJoinFilter.SemiJoinType.COL_NAME_ROW;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HBaseBloomFilterSemiJoinSystemTest {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseBloomFilterSemiJoinSystemTest.class);

  private static final byte[] TABLE = toBytes("test_table");
  private static final byte[] ROW_COLBF_CF = toBytes("cf1");
  private static final int NUM_ROWS = 100000;
  private static HTable table;
  private static HBaseTestingUtility util;

  public static void setUp() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    util = new HBaseTestingUtility(conf);

  }

  @BeforeClass
  public static void startCluster() throws Exception {
    setUp();
    util.startMiniCluster(3);
    try {
      HTableDescriptor testTableDesc = new HTableDescriptor(TABLE);
      HColumnDescriptor cf1 = new HColumnDescriptor(ROW_COLBF_CF);
      cf1.setBloomFilterType(StoreFile.BloomType.ROWCOL);
      testTableDesc.addFamily(cf1);
      testTableDesc.addCoprocessor(NonAggregatingRegionObserver.class.getName());
      util.getHBaseAdmin().createTable(testTableDesc);
      HTable testTable = new HTable(util.getConfiguration(), TABLE);
      util.createMultiRegions(util.getConfiguration(), testTable, ROW_COLBF_CF, new byte[][]{util.KEYS[0], util.KEYS[1], util.KEYS[2]}, true);
      testTable.close();
      table = new HTable(util.getConfiguration(), TABLE);
      // if we just created the table load the data
      loadData();
    } catch (TableExistsException tee) {
    }
    table = new HTable(util.getConfiguration(), TABLE);
  }

  public static void loadData() throws IOException, InterruptedException {
    // load a 1000 into each region, regions start on "" "bbb" and "ccc"
    table.setAutoFlush(false);
    for (int i = 0; i < NUM_ROWS; i++) {
      byte[] key1 = toBytes("aaa" + i);
      byte[] key2 = toBytes("bbb" + i);
      byte[] key3 = toBytes("ccc" + i);
      // the keys to insert, one per region, the other keys become the col_name and col_value on the other puts
      Put r1Put = new Put(key1);
      r1Put.add(ROW_COLBF_CF, key2, key3);
      Put r2Put = new Put(key2);
      r2Put.add(ROW_COLBF_CF, key1, key3);
      // only fill the last "half" of the region
      if (i > NUM_ROWS / 2) {
        Put r3Put = new Put(key3);
        r3Put.add(ROW_COLBF_CF, key2, key1);
        table.put(ImmutableList.of(r1Put, r2Put, r3Put));
      } else {
        table.put(ImmutableList.of(r1Put, r2Put));
      }

    }
    // flush the puts, flush the table and major compact.
    table.flushCommits();
    table.close();
    util.flush(TABLE);
    util.compact(TABLE, true);
  }


  @Test
  public void testBloomFilterSemiJoinDirectly() throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    NavigableMap<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>> regionIndex =
      NonAggregatingRegionObserver.buildIndex("test_table",
        util.getConfiguration(),
        util.getTestFileSystem(),
        new Path(util.getDefaultRootDirPath() + Path.SEPARATOR + "test_table"));

    assertSame("Unexpected number of regions.", 3, regionIndex.size());

    NavigableMap<HRegionInfo, ServerName> regions = table.getRegionLocations();
    for (Map.Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
      LOG.info("Using Region: " + entry.getKey() + " Server: " + entry.getValue());
    }

    for (Map.Entry<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>> entry : regionIndex.entrySet()) {
      assertSame("Unexpected number of HFiles.", 1, entry.getValue().get(ByteBuffer.wrap(ROW_COLBF_CF)).size());
    }

    CompoundBloomFilterBase bfEntryCreator = new CompoundBloomFilterBase();
    double falsePositivesCounter = 0.0;

    for (int i = 0; i < NUM_ROWS; i++) {
      byte[] key1 = toBytes("aaa" + i);
      byte[] key2 = toBytes("bbb" + i);
      byte[] key3 = toBytes("ccc" + i);

      assertNotNull("Could not find a region for key: " + new String(key2));

      // creates bbbXaaaX bf entry keys that must match region0 [bbb0, bbb999] ROW_COL BF [bbb0aaa0, bbb999aaa999]
      byte[] bfMatchKey = bfEntryCreator.createBloomKey(key2, 0, key2.length, key1, 0, key1.length);
      BloomFilter bfMatch = bloomFilterForRowCol(regionIndex, key2);

      // bloom filters don't return false positives
      assertTrue("Unexpected result from the bloom filter: " + new String(bfMatchKey),
        bfMatch.contains(bfMatchKey, 0, bfMatchKey.length, null));

      // creates bbbXcccX bf entry keys that don't exist in region2 [bbb0, bbb999] ROW_COL BF [bbb0aaa0, bbb999aaa999]
      // but will match the index and therefore may provide false positives
      byte[] bfNoMatchKeyFalsePositives = bfEntryCreator.createBloomKey(key2, 0, key2.length, key3, 0, key3.length);
      BloomFilter bfNoMatchFalsePositives = bloomFilterForRowCol(regionIndex, key2);

      if (bfNoMatchFalsePositives.contains(bfNoMatchKeyFalsePositives, 0, bfNoMatchKeyFalsePositives.length, null)) {
        falsePositivesCounter++;
      }

      if (i <= NUM_ROWS / 2 && Integer.parseInt((i + "").charAt(0) + "") < 5) {
        // creates cccXaaaX bf entry keys that don't exist in region3 [ccc0, ccc999] ROW_COL BF [ccc0bbb0, ccc999bbb999]
        // but won't match the index (and therefore won't provide false positives)
        byte[] bfNoMatchKeyNoFalsePositives = bfEntryCreator.createBloomKey(key3, 0, key3.length, key1, 0, key1.length);
        BloomFilter bfNoMatchNoFalsePositives = bloomFilterForRowCol(regionIndex, key3);
        assertFalse("Unexpected result from the bloom filter: " + new String(bfNoMatchKeyNoFalsePositives),
          bfNoMatchNoFalsePositives.contains(bfNoMatchKeyNoFalsePositives, 0, bfNoMatchKeyNoFalsePositives.length, null));
      }
    }
    double falsePositiveRate = falsePositivesCounter / NUM_ROWS;
    LOG.info("False positive Rate: {} ", falsePositiveRate);
    assertTrue("Unexpectedly high percentage of false positives: " + falsePositiveRate, falsePositiveRate < 0.1);
  }

  private void testSerializeFilter(Filter filter) throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutput output = new DataOutputStream(outputStream);
    filter.write(output);
    byte[] streamed = outputStream.toByteArray();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(streamed);
    DataInput input = new DataInputStream(inputStream);
    Filter newFilter = filter.getClass().newInstance();
    newFilter.readFields(input);
  }

  @Test
  public void testBloomFilterSemiJoinScanMatch() throws Exception {
    BloomFilterSemiJoinFilter matchFilter = new BloomFilterSemiJoinFilter(COL_NAME_ROW, ROW_COLBF_CF, new ColumnPrefixFilter(toBytes("aaa")));
    // make sure the filter can be serialized
    testSerializeFilter(matchFilter);

    // test the scan on the region that has a BF match, should return the complete set of rows for that region
    Scan filterMatchScan = new Scan();
    filterMatchScan.setStartRow(HBaseTestingUtility.KEYS[1]);
    filterMatchScan.setStopRow(HBaseTestingUtility.KEYS[2]);
    filterMatchScan.setFilter(matchFilter);
    ResultScanner matchScanner = table.getScanner(filterMatchScan);
    Result[] matchScannerResults = matchScanner.next(NUM_ROWS);
    assertEquals("BF match scanner did not return the expected number of results", NUM_ROWS, matchScannerResults.length);
  }

  @Test
  public void testBloomFilterSemiJoinScanNoMatch() throws Exception {
    // scans the "ccc" region looking for "bbbccc" bf entries (which don't exist) may provide false positives
    BloomFilterSemiJoinFilter noMatchFilter = new BloomFilterSemiJoinFilter(COL_NAME_ROW, ROW_COLBF_CF, new ColumnPrefixFilter(toBytes("bbb")));
    testSerializeFilter(noMatchFilter);
    Scan noMatchScan = new Scan();
    noMatchScan.setStartRow(HBaseTestingUtility.KEYS[2]);
    noMatchScan.setStopRow(HBaseTestingUtility.KEYS[3]);
    noMatchScan.setFilter(noMatchFilter);
    ResultScanner noMatchScanner = table.getScanner(noMatchScan);
    Result[] noMatchScannerResults = noMatchScanner.next(NUM_ROWS);
    double falsePositives = noMatchScannerResults.length / ((NUM_ROWS - 1) / 2.0);
    LOG.info("False positive rate: " + falsePositives + " count: " + noMatchScannerResults.length);
    assertTrue("Unexpectedly high percentage of false positives: " + falsePositives, falsePositives < 0.1);
  }


  @Test
  public void testBloomFilterSemiJoinWithScanMatchOne() throws Exception {
    // scans the "bbb" region looking for "aaaXbbb0" entries, must provide at least one value (i.e. aaa0bbb0 exists,
    // if more the rest are false positives)
    BloomFilterSemiJoinFilter onlyMatchOneFilter = new BloomFilterSemiJoinFilter(COL_NAME_ROW, ROW_COLBF_CF,
      new ColumnPrefixFilter(toBytes("aaa")), new BloomFilterSemiJoinFilter.StaticColumnBloomFilterLookupKeyBuilder(toBytes("bbb0")));
    testSerializeFilter(onlyMatchOneFilter);

    Scan onlyMatchOneScan = new Scan();
    onlyMatchOneScan.setStartRow(HBaseTestingUtility.KEYS[1]);
    onlyMatchOneScan.setStopRow(HBaseTestingUtility.KEYS[2]);
    onlyMatchOneScan.setFilter(onlyMatchOneFilter);
    ResultScanner onlyMatchOneScanner = table.getScanner(onlyMatchOneScan);
    Result[] onlyMatchOneResults = onlyMatchOneScanner.next(NUM_ROWS);
    LOG.info("Positive count: " + onlyMatchOneResults.length);
    assertTrue("At least one match was expected: " + onlyMatchOneResults.length, onlyMatchOneResults.length >= 1);
  }

  @Test
  public void testBloomFilterSemiJoinWithScanMatchRowKey() throws Exception {
    // scans the "bbb" region looking for "bbbXaaa1" entries, must provide at least one value (i.e. aaa0bbb0 exists,
    // if more the rest are false positives)
    BloomFilterSemiJoinFilter onlyMatchOneFilter = new BloomFilterSemiJoinFilter(COL_NAME_ROW, ROW_COLBF_CF,
      new BloomFilterSemiJoinFilter.StaticRowBloomFilterLookupKeyBuilder(toBytes("aaa1")));
    testSerializeFilter(onlyMatchOneFilter);

    Scan onlyMatchOneScan = new Scan();
    onlyMatchOneScan.setStartRow(HBaseTestingUtility.KEYS[1]);
    onlyMatchOneScan.setStopRow(HBaseTestingUtility.KEYS[2]);
    onlyMatchOneScan.setFilter(onlyMatchOneFilter);
    ResultScanner onlyMatchOneScanner = table.getScanner(onlyMatchOneScan);
    Result[] onlyMatchOneResults = onlyMatchOneScanner.next(NUM_ROWS);
    LOG.info("Positive count: " + onlyMatchOneResults.length);
    assertTrue("At least one match was expected: " + onlyMatchOneResults.length, onlyMatchOneResults.length >= 1);
  }

  private BloomFilter bloomFilterForRowCol(NavigableMap<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>> regionIndex,
    byte[] key) {
    Map.Entry<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>> entry =
      regionIndex.floorEntry(ByteBuffer.wrap(key));
    Collection<BloomFilter> result = entry.getValue().get(ByteBuffer.wrap(ROW_COLBF_CF));
    assertNotNull("Could not find BloomFilter for CF: " + new String(ROW_COLBF_CF));
    assertSame("Unexpected number of BloomFilters for CF.", 1, result.size());
    return Iterables.get(result, 0);
  }


  @AfterClass
  public static void stopCluster() throws Exception {
    util.deleteTable(TABLE);
    util.shutdownMiniCluster();
  }

  private static void listFiles(FileSystem fs, Path path) throws IOException {
    for (FileStatus status : fs.listStatus(path)) {
      LOG.info(status.getPath().toString());
      if (status.isDir()) {
        listFiles(fs, status.getPath());
      }
    }
  }
}
