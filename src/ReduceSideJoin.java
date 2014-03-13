/**
 * Reduce Side Join Template for BSBM Benchmark
 * @date March 2013
 * @author Albert Haque
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class ReduceSideJoin {

	private static String HBASE_TABLE_NAME = "rdf1";
	private byte[] COLUMN_FAMILY_AS_BYTES = "p".getBytes();
	private byte[] fakeValue = Bytes.toBytes("DOESNOTEXIST");
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		//String USAGE_MSG = "  Arguments: <zk quorum>";

//		if (args == null || args.length != 1) {
//			System.out.println(USAGE_MSG);
//			System.exit(0);
//		}
		startJob(args);
	}
	
	@SuppressWarnings("deprecation")
	public static Job startJob(String[] args) throws IOException {
		
		// args[0] = zookeeper
		// args[1] = hbase table name
		
		Configuration hConf = HBaseConfiguration.create(new Configuration());
	    hConf.set("hbase.zookeeper.quorum", "localhost");
	    hConf.set("scan.table", HBASE_TABLE_NAME);
	    hConf.set("hbase.zookeeper.property.clientPort", "2181");

	    Scan scan = new Scan();
	    scan.setFilter(rowColBloomFilter());
		
		Job job = new Job(hConf);
		job.setJobName("Reduce Side Join");
		job.setJarByClass(ReduceSideJoin.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
		scan.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				HBASE_TABLE_NAME,        // input HBase table name
				scan,             // Scan instance to control CF and attribute selection
				ReduceSideJoin_Mapper.class,   // mapper
				Text.class,         // mapper output key
				IntWritable.class,  // mapper output value
				job);

		// Reducer settings
		job.setReducerClass(ReduceSideJoin_Reducer.class);    // reducer class
		job.setNumReduceTasks(1);    // at least one, adjust as required
		
		FileOutputFormat.setOutputPath(job, new Path("output/2014-03-12_output.txt"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	private static Filter rowColBloomFilter() {
		// the base BD filter, either offers or reviews must have the product as column to be considered
		Filter filter = new BloomFilterSemiJoinFilter(COL_NAME_ROW,
		BSBMDataSetProcessor.COLUMN_FAMILY_AS_BYTES, new BloomFilterSemiJoinFilter.StaticRowBloomFilterLookupKeyBuilder(productName));
		return new FilterList(FilterList.Operator.MUST_PASS_ALL, filter, new FilterList(FilterList.Operator.MUST_PASS_ONE, reviewsFilter(), offersFilter()));
	}
	
	private Filter reviewsFilter() {
		// select only the reviews that have product as a column name
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				BSBMDataSetProcessor.COLUMN_FAMILY_AS_BYTES, productName,
				CompareFilter.CompareOp.NOT_EQUAL, fakeValue);
		filter.setFilterIfMissing(true);
		return filter;

	}

	private Filter offersFilter() {
		// select only the rows that have product as a column value.
		SingleColumnValueFilter excludeIfNoProduct = new SingleColumnValueFilter(
				COLUMN_FAMILY_AS_BYTES, productName,
				CompareFilter.CompareOp.NOT_EQUAL, fakeValue);
		excludeIfNoProduct.setFilterIfMissing(true);

		// of those select only the ones for whom the BF matches vendor
		// (dc_publisher) -> country
		BloomFilterSemiJoinFilter bfFilter = new BloomFilterSemiJoinFilter(
				COL_NAME_ROW,
				BSBMDataSetProcessor.COLUMN_FAMILY_AS_BYTES,
				new ColumnPrefixFilter(productName),
				new BloomFilterSemiJoinFilter.StaticColumnBloomFilterLookupKeyBuilder(
						country));

		return new FilterList(FilterList.Operator.MUST_PASS_ALL,
				excludeIfNoProduct, bfFilter);
	}
	
	public static class ReduceSideJoin_Mapper extends TableMapper<Text, IntWritable> {
		
		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			// Get all key-values for a row
		    Cell[] cells = value.rawCells();
		    for (Cell kv : cells) {
		    	String object = new String(CellUtil.cloneQualifier(kv));
		    	text.set(object);
		    	context.write(text, ONE);
		    }
		}
	}
	
	public static class ReduceSideJoin_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			context.write(key, new IntWritable(i));
		}
		
	}
	    
		    
}
