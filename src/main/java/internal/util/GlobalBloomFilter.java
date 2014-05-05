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
package internal.util;

import com.google.common.collect.ListMultimap;
import org.apache.hadoop.hbase.util.BloomFilter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Data structure used by Bloom semi-join, that maps to the complete set of all bloom filters in HBase.
 * <p/>
 * The major goal is help in implementing key<->column name joins by allowing a filter only to return the
 * columns that *might* exist as keys in the same table (cross table joins are not implemented as of now).
 * <p/>
 * The data structure is laid out the following way:
 * <p/>
 * 1 - Entry point is an index of all Regions existing at the time of creation, the RegionIndex.
 * 2 - Each entry in the region index is a map of ColumnFamilies to a list of CompoundBloomFilters
 * one for each HFile in the region.
 * <p/>
 * During initialization the Filter receives a complete map of all of HBases HFiles. Actually loading the
 * the Bloom Filters from the HFiles is don on-demand and cached in the block cache for further reuse.
 */
public class GlobalBloomFilter {

  /**
   * Index of regions to RegionColumnFamilyBloomFilter's. The value contains the compound bloom filters for the
   * relevant column families (set at initialization time).
   * The list may have one or more elements depending on the number of available CF's and on the CF's specified
   * in the query.
   */
  NavigableMap<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>> regionIndex;

  public GlobalBloomFilter(NavigableMap<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>> regionIndex) throws IOException {
    this.regionIndex = regionIndex;
  }

  /**
   * Tests if any BF contains the key.
   */
  public boolean contains(byte[] cf, int cfOffset, int cfLength,
    // the row key (to select the region)
    byte[] rowKey, int rowKeyOffset, int rowKeyLength,
    // the bf match we're searching for
    byte[] bfKey, int bfKeyOffset, int bfKeyLength) {

    ByteBuffer wrappedKey = ByteBuffer.wrap(rowKey, rowKeyOffset, rowKeyLength);
    ByteBuffer wrappedCF = ByteBuffer.wrap(cf, cfOffset, cfLength);
    Map.Entry<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>> entry = regionIndex.floorEntry(wrappedKey);

    // entry is not within region bounds
    if (entry == null) {
      return false;
    }
    Collection<BloomFilter> filters = entry.getValue().get(wrappedCF);
    // the region has no HFiles
    if (filters == null) {
      return false;
    }

    // got through all the bfs of all the HFiles
    for (BloomFilter filter : filters) {
      if (filter.contains(bfKey, bfKeyOffset, bfKeyLength, null)) {
        return true;
      }
    }
    return false;
  }


}
