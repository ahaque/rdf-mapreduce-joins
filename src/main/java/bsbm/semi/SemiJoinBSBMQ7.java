package bsbm.semi;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import internal.filters.BloomFilterSemiJoinFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import tools.BSBMDataSetProcessor;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static internal.filters.BloomFilterSemiJoinFilter.SemiJoinType.COL_NAME_ROW;

/**
 * @author David Alves
 */
public class SemiJoinBSBMQ7 extends Configured implements Tool {

  public enum Counters {
    PROCESSES_KVS
  }

  private static final Log LOG = LogFactory.getLog(SemiJoinBSBMQ7.class);

  static String offersPrefix = "bsbm-inst_dataFromVendor";
  static String reviewsPrefix = "bsbm-inst_dataFromRatingSite";
  static String reviewer_col = "rev_reviewer";
  static String publisher = "dc_publisher";
  static String vendor = "bsbm-voc_vendor";
  static byte[] label = Bytes.toBytes("rdfs_label");
  static byte[] country = Bytes.toBytes("<http://downlode.org/rdf/iso-3166/countries#DE>");
  static byte[] reviewer_name = Bytes.toBytes("foaf_name");
  static byte[] fakeValue = Bytes.toBytes("DOESNOTEXIST");

  public static class Query7Mapper extends TableMapper<Text, BSBMQueries.KeyValuArrayWritable> {

    Result product;
    HTable table;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      table = new HTable(conf, conf.get("scan.table"));
      product = table.get(new Get(Bytes.toBytes(conf.get("query.product"))));
      checkNotNull(product);
    }

    /**
     * We do a single map scan that scans offers and reviews and does a map side join with vendors and reviewers, respectively.
     * We then do a reduce side join to join offers and reviewers
     */
    @Override
    protected void map(ImmutableBytesWritable rowKey, Result value, Context context) throws IOException, InterruptedException {
      //      context.write(new Text(rowKey.get()), new KeyValuArrayWritable(value.raw()));
      context.getCounter(Counters.PROCESSES_KVS).increment(value.raw().length);
      //      System.out.println(new String(rowKey.get()) + " : " + value.toString());
      if (isOffer(rowKey)) {
        processOffer(rowKey, value, context);
      } else if (isReview(rowKey)) {
        processReview(rowKey, value, context);
      }
    }

    private boolean isOffer(ImmutableBytesWritable rowKey) {
      String keyAsString = new String(rowKey.get());
      return keyAsString.startsWith(offersPrefix) && keyAsString.contains("Offer");
    }

    private boolean isReview(ImmutableBytesWritable rowKey) {
      String keyAsString = new String(rowKey.get());
      return keyAsString.startsWith(reviewsPrefix) && keyAsString.contains("Review");
    }


    /**
     * Process the offer by getting the vendor and adding it to the result
     *
     * @param rowKey
     * @param offer
     * @param context
     */
    private void processOffer(ImmutableBytesWritable rowKey, Result offer, Context context) throws IOException, InterruptedException {
      // do the map side join with vendor
      System.out.println("Processing offer: " + rowKey + " Result: " + offer);
      Result vendor = null;

      byte[] vendorKey = getVendor(offer.raw());
      vendor = table.get(new Get(vendorKey));
      if (vendor.getColumn(BSBMDataSetProcessor.COLUMN_FAMILY_AS_BYTES, country).isEmpty()) {
        return;
      }
      if (vendor == null) {
        System.out.println("No Vendor for: " + new String(vendorKey) + " " + offer.raw());
        return;
      }
      // merge the vendor, the offer and the product
      KeyValue[] allResults = new KeyValue[offer.raw().length + vendor.raw().length + product.raw().length];
      System.arraycopy(offer.raw(), 0, allResults, 0, offer.raw().length);
      System.arraycopy(vendor.raw(), 0, allResults, offer.raw().length, vendor.raw().length);
      System.arraycopy(product.raw(), 0, allResults, offer.raw().length + vendor.raw().length, product.raw().length);
      context.write(new Text(product.getRow()), new BSBMQueries.KeyValuArrayWritable(allResults));
      //      LOG.info("Wrote offer: " + product.getRow() + " Result: " + writable);
    }

    private byte[] getVendor(KeyValue[] values) {
      for (KeyValue kv : values) {
        String value = new String(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
        if (value.equals(publisher) || value.equals(vendor)) {
          return kv.getQualifier();
        }
      }
      // shouldn't happen
      return null;
    }

    /**
     * Process the review by getting the reviewer and adding it to the result.
     */
    private void processReview(ImmutableBytesWritable rowKey, Result review, Context context) throws IOException, InterruptedException {
      // do the map side join with reviewer
      System.out.println("Processing review: " + rowKey + " Result: " + review);

      byte[] reviewerKey = getReviewer(review.raw());
      Result reviewer = table.get(new Get(reviewerKey).addColumn(BSBMDataSetProcessor.COLUMN_FAMILY_AS_BYTES, reviewer_name));
      if (reviewer == null) {
        System.out.println("No Reviewer for: " + new String(reviewerKey) + " " + review.raw());
        return;
      }
      // merge the review the reviewer and the product
      KeyValue[] allResults = new KeyValue[review.raw().length + reviewer.raw().length];
      System.arraycopy(review.raw(), 0, allResults, 0, review.raw().length);
      System.arraycopy(reviewer.raw(), 0, allResults, review.raw().length, reviewer.raw().length);
      //      System.arraycopy(product.raw(), 0, allResults, review.raw().length + reviewer.raw().length, product.raw().length);
      context.write(new Text(product.getRow()), new BSBMQueries.KeyValuArrayWritable(allResults));
      //      LOG.info("Wrote review: " + product.getRow() + " Result: " + writable);
    }

    private byte[] getReviewer(KeyValue[] values) {
      for (KeyValue kv : values) {
        String value = new String(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
        if (value.equals(reviewer_col)) {
          return kv.getQualifier();
        }
      }
      // shouldn't happen
      return null;
    }

  }

  private byte[] productName;

  public static void main(String[] args) throws Exception {
    new SemiJoinBSBMQ7().run(args);
  }

  @Override
  public int run(String[] args) throws Exception {

    Configuration hConf = HBaseConfiguration.create(new Configuration());
    hConf.set("hbase.zookeeper.quorum", args[0]);
    hConf.set("scan.table", args[1]);
    productName = Bytes.toBytes(args[2]);
    hConf.set("query.product", args[2]);
    hConf.set("hbase.zookeeper.property.clientPort", "2181");

    Scan scan = new Scan();
    scan.setFilter(filter());

    Job job = new Job(hConf, "Execute BSBM Query7 data in " + args[1]);
    job.setJarByClass(SemiJoinBSBMQ7.class);
    TableMapReduceUtil.initTableMapperJob(args[1], scan, SemiJoinBSBMQ7.Query7Mapper.class,
      Text.class, BSBMQueries.KeyValuArrayWritable.class, job);
    TableMapReduceUtil.addDependencyJars(hConf,
      org.apache.zookeeper.ZooKeeper.class,
      com.google.protobuf.Message.class,
      job.getMapOutputKeyClass(),
      job.getMapOutputValueClass(),
      job.getInputFormatClass(),
      job.getOutputKeyClass(),
      job.getOutputValueClass(),
      job.getOutputFormatClass(),
      job.getPartitionerClass(),
      job.getCombinerClass(),
      BloomFilterSemiJoinFilter.class);
    job.setReducerClass(BSBMQueries.JoinReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

    // get the product and store it in hdfs
    // providing it as one of the MR job's input

    return 0;
  }

  /**
   * Provide a filter that selects offers and reviews, these are joined map side with product and vendor/reviewer
   * and then joined with each other reduce side.
   *
   * @return
   */
  private Filter filter() {
    // the base BD filter, either offers or reviews must have the product as column to be considered
    Filter filter = new BloomFilterSemiJoinFilter(COL_NAME_ROW,
      BSBMDataSetProcessor.COLUMN_FAMILY_AS_BYTES, new BloomFilterSemiJoinFilter.StaticRowBloomFilterLookupKeyBuilder(productName));
    return new FilterList(FilterList.Operator.MUST_PASS_ALL, filter, new FilterList(FilterList.Operator.MUST_PASS_ONE, reviewsFilter(), offersFilter()));
  }

  private Filter reviewsFilter() {
    // select only the reviews that have product as a column name
    SingleColumnValueFilter filter = new SingleColumnValueFilter(BSBMDataSetProcessor.COLUMN_FAMILY_AS_BYTES,
      productName, CompareFilter.CompareOp.NOT_EQUAL, fakeValue);
    filter.setFilterIfMissing(true);
    return filter;

  }


  private Filter offersFilter() {
    // select only the rows that have product as a column value.
    SingleColumnValueFilter excludeIfNoProduct = new SingleColumnValueFilter(BSBMDataSetProcessor.COLUMN_FAMILY_AS_BYTES,
      productName, CompareFilter.CompareOp.NOT_EQUAL, fakeValue);
    excludeIfNoProduct.setFilterIfMissing(true);

    // of those select only the ones for whom the BF matches vendor (dc_publisher) -> country
    BloomFilterSemiJoinFilter bfFilter = new BloomFilterSemiJoinFilter(COL_NAME_ROW,
      BSBMDataSetProcessor.COLUMN_FAMILY_AS_BYTES, new ColumnPrefixFilter(productName),
      new BloomFilterSemiJoinFilter.StaticColumnBloomFilterLookupKeyBuilder(country));

    return new FilterList(FilterList.Operator.MUST_PASS_ALL, excludeIfNoProduct, bfFilter);
  }
}
