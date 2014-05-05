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
package internal.coprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ListMultimap;
import internal.filters.BloomFilterSemiJoinFilter;
import internal.util.GlobalBloomFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

/**
 * Provides server-side help for gets/scans/puts.
 * Current feature set is:
 * - sets the current regionIndex to the BloomFilterSemiJoinFilter if one is provided (for gets/scans)
 */
public class NonAggregatingRegionObserver extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(NonAggregatingRegionObserver.class);

  /**
   * Builds the regionIndex by going through all the store files for each region and creating a reader for it that can
   * later be used to read the bloom filters, on demand.
   */
  @VisibleForTesting
  public static NavigableMap<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>> buildIndex(String tableName,
    Configuration conf,
    FileSystem fileSystem,
    Path tableBasePath)
    throws IOException {
    ImmutableSortedMap.Builder<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>> indexBuilder =
      ImmutableSortedMap.naturalOrder();
    try {
      HTable table = new HTable(conf, tableName);
      NavigableMap<HRegionInfo, ServerName> regions = table.getRegionLocations();
      Collection<HColumnDescriptor> families = table.getTableDescriptor().getFamilies();
      table.close();
      LOG.info("Building RegionIndex [Table: " + tableName + " Base Path: " + tableBasePath.toString() + "]");
      for (HRegionInfo region : regions.keySet()) {
        ImmutableListMultimap.Builder<ByteBuffer, BloomFilter> familiesAndStoreFiles =
          ImmutableListMultimap.builder();
        for (HColumnDescriptor family : families) {
          Path path = new Path(tableBasePath, region.getEncodedName() + Path.SEPARATOR + family.getNameAsString());
          FileStatus[] storeFilesStatuses = fileSystem.listStatus(path);
          for (FileStatus status : storeFilesStatuses) {
            LOG.info("Processing Store File: " + status.getPath().toString() + " Column Family: " +
              family.getNameAsString() + " for Region: " + region.getRegionNameAsString());
            // maybe increased the cache for this reader since it's be only reading blooms?
            HFile.Reader reader = HFile.createReader(fileSystem, status.getPath(), new CacheConfig(conf));
            familiesAndStoreFiles.put(ByteBuffer.wrap(family.getName()),
              BloomFilterFactory.createFromMeta(reader.getGeneralBloomFilterMetadata(), reader));
          }
        }
        indexBuilder.put(ByteBuffer.wrap(region.getStartKey()), familiesAndStoreFiles.build());
      }
      return indexBuilder.build();
    } catch (Exception e) {
      LOG.error("Could not load regionIndex, BloomFilterSemiJoinFilter will not work.", e);
    }
    return null;
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    RegionCoprocessorEnvironment env = e.getEnvironment();

    try {
      List<BloomFilterSemiJoinFilter> filters = findSemiJoinFilters(scan.getFilter(), ImmutableList.<BloomFilterSemiJoinFilter>builder());
      if (!filters.isEmpty()) {
        LOG.info("Found BloomFilterSemiJoinFilters in scan");
        String tableName = env.getRegion().getTableDesc().getNameAsString();
        NavigableMap<ByteBuffer, ListMultimap<ByteBuffer, BloomFilter>> thisTableIndex;
        synchronized (TABLE_INDEXES_LOCK) {
          thisTableIndex = TABLE_INDEXES.get(tableName);
          if (thisTableIndex == null) {
            LOG.info("Region BloomFilter Index was unavailable for table [" + tableName + "] , building...");
            thisTableIndex = buildIndex(tableName, env.getConfiguration(), env.getRegionServerServices().getFileSystem(),
              env.getRegion().getTableDir());
            TABLE_INDEXES.put(tableName, thisTableIndex);
            LOG.info("Region Index built for table [" + tableName + "]and made available for all Regions.");
          }
        }
        for (BloomFilterSemiJoinFilter filter : filters) {
          HColumnDescriptor cf = env.getRegion().getTableDesc().getFamily(filter.getColumnFamily());
          if (cf == null) {
            LOG.warn("BloomFilterSemiJoinFilter refers to a non-existing column family (" +
              new String(filter.getColumnFamily()) + "). Filtering will not be performed.");
          } else {
            StoreFile.BloomType bloomType = cf.getBloomFilterType();
            // make sure the requested semi join matches the available bloom filters
            switch (filter.getSemiJoinType()) {
              case COL_NAME:
              case COL_VALUE:
                if (bloomType == StoreFile.BloomType.ROW) {
                  filter.setFilter(new GlobalBloomFilter(thisTableIndex));
                  LOG.info("Proceeding with BloomFiltered " + filter.getSemiJoinType() + " scan on CF: " +
                    cf.getNameAsString());
                } else {
                  LOG.warn("Wrong type of BloomFilterSemiJoin requested, Filter required: " + filter.getSemiJoinType() +
                    " but the Column Family has: " + bloomType);
                }
                break;
              case COL_NAME_ROW:
                if (bloomType == StoreFile.BloomType.ROWCOL) {
                  filter.setFilter(new GlobalBloomFilter(thisTableIndex));
                  LOG.info("Proceeding with BloomFiltered ROW_COL scan on CF: " + cf.getNameAsString());
                } else {
                  LOG.warn("Wrong type of BloomFilterSemiJoin requested, Filter required: " + filter.getSemiJoinType() +
                    " but the Column Family has: " + bloomType);
                }
                break;
            }
          }
        }
      }
    } catch (IOException e1) {
      LOG.error("ERROR", e1);
    }
    return super.preScannerOpen(e, scan, s);
  }

  private List<BloomFilterSemiJoinFilter> findSemiJoinFilters(Filter filter, ImmutableList.Builder<BloomFilterSemiJoinFilter> list) {
    if (filter != null) {
      if (BloomFilterSemiJoinFilter.class.isAssignableFrom(filter.getClass())) {
        list.add((BloomFilterSemiJoinFilter) filter);
      }
      if (FilterList.class.isAssignableFrom(filter.getClass())) {
        findSemiJoinFilterInList((FilterList) filter, list);
      }
    }
    return list.build();
  }

  private void findSemiJoinFilterInList(FilterList list, ImmutableList.Builder<BloomFilterSemiJoinFilter> builder) {
    for (Filter subFilter : list.getFilters()) {
      if (BloomFilterSemiJoinFilter.class.isAssignableFrom(subFilter.getClass()))
        builder.add((BloomFilterSemiJoinFilter) subFilter);
      if (FilterList.class.isAssignableFrom(subFilter.getClass())) {
        findSemiJoinFilterInList((FilterList) subFilter, builder);
      }
    }
  }
}
