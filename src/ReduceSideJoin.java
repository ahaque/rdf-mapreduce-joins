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
import org.apache.hadoop.hbase.filter.FilterList;
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
	
	// Query Information
	private static String ProductFeature1 = "hello";
	private static String ProductFeature2 = "hello";
	private static String ProductType = "hello";
	private static double x = 0.0;

	private byte[] COLUMN_FAMILY_AS_BYTES = "o".getBytes();
	private byte[] fakeValue = Bytes.toBytes("DOESNOTEXIST");
	private byte[] productName = Bytes.toBytes("product_name_here");
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		String USAGE_MSG = "  Arguments: <table name> <zk quorum>";

		if (args == null || args.length != 2) {
			System.out.println("\n" + USAGE_MSG);
			System.out.println("You entered " + args.length + " arguments.");
			System.exit(0);
		}
		startJob(args);
	}
	
	@SuppressWarnings("deprecation")
	public static Job startJob(String[] args) throws IOException {
		
		// args[0] = hbase table name
		// args[1] = zookeeper
		
		Configuration hConf = HBaseConfiguration.create(new Configuration());
	    hConf.set("hbase.zookeeper.quorum", args[1]);
	    hConf.set("scan.table", args[0]);
	    hConf.set("hbase.zookeeper.property.clientPort", "2181");

	    Scan scan = new Scan();
	    //scan.setFilter(rowColBloomFilter());
		
		Job job = new Job(hConf);
		job.setJobName("Reduce Side Join");
		job.setJarByClass(ReduceSideJoin.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
		scan.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],        // input HBase table name
				scan,             // Scan instance to control CF and attribute selection
				ReduceSideJoin_Mapper.class,   // mapper
				Text.class,         // mapper output key
				IntWritable.class,  // mapper output value
				job);

		// Reducer settings
		job.setReducerClass(ReduceSideJoin_Reducer.class);    // reducer class
		job.setNumReduceTasks(1);    // at least one, adjust as required
		
		FileOutputFormat.setOutputPath(job, new Path("output/2014-03-13"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
//	private static Filter rowColBloomFilter() {
//		// the base BD filter, either offers or reviews must have the product as column to be considered
//		Filter filter = new BloomFilterSemiJoinFilter(COL_NAME_ROW,
//		BSBMDataSetProcessor.COLUMN_FAMILY_AS_BYTES, new BloomFilterSemiJoinFilter.StaticRowBloomFilterLookupKeyBuilder(productName));
//		return new FilterList(FilterList.Operator.MUST_PASS_ALL, filter, new FilterList(FilterList.Operator.MUST_PASS_ONE, reviewsFilter(), offersFilter()));
//	}
	
	private Filter reviewsFilter() {
		// select only the reviews that have product as a column name
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				COLUMN_FAMILY_AS_BYTES, productName,
				CompareFilter.CompareOp.NOT_EQUAL, fakeValue);
		filter.setFilterIfMissing(true);
		return filter;
	}

	private Filter offersFilter() {
		// select only the rows that have product as a column value.
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				COLUMN_FAMILY_AS_BYTES, productName,
				CompareFilter.CompareOp.NOT_EQUAL, fakeValue);
		filter.setFilterIfMissing(true);
		return filter;
	}
	
	public static class ReduceSideJoin_Mapper extends TableMapper<Text, IntWritable> {
		
		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			// BSBM Query 1. Subject must satisfy all triple patterns
			// Non equality checks in the query appear as true here
			boolean[] queryTriplePatterns = new boolean[6];
			queryTriplePatterns[0] = true;
			queryTriplePatterns[1] = false;
			queryTriplePatterns[2] = false;
			queryTriplePatterns[3] = false;
			queryTriplePatterns[4] = true;
			queryTriplePatterns[5] = false;
			
			// Get all key-values for a row
		    Cell[] cells = value.rawCells();
		    StringBuilder predicate = new StringBuilder();
		    StringBuilder object = new StringBuilder();
		    for (Cell kv : cells) {

		    }
		    text.set(object.toString());
	    	context.write(text, ONE);
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
