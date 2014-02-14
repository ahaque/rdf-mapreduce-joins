import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;


public class ReduceSideJoin {

	static String HBASE_TABLE_NAME = "rdf1";
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		String USAGE_MSG = "  Arguments: <zk quorum>";

//		if (args == null || args.length != 1) {
//			System.out.println(USAGE_MSG);
//			System.exit(0);
//		}

		Job job = startJob(args);
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
		
		Job job = new Job(hConf);
		job.setJobName("HBaseScan");
		job.setJarByClass(ReduceSideJoin.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
		scan.setCacheBlocks(false);
		
		TableMapReduceUtil.initTableMapperJob(
				HBASE_TABLE_NAME,        // input HBase table name
				scan,             // Scan instance to control CF and attribute selection
				MyMapper.class,   // mapper
				null,             // mapper output key
				null,             // mapper output value
				job);
		//job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper
		
		TableMapReduceUtil.initTableReducerJob(
				HBASE_TABLE_NAME,        // output table
				MyTableReducer.class,    // reducer class
				job);
		
		FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/mySummaryFile"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	public static class MyMapper extends TableMapper<Text, IntWritable> {
		
		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			String val = new String(value.getValue("p".getBytes(), "dc_date".getBytes()));
			text.set(val);
			context.write(text, ONE);
		}
	}
	
	public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>  {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			context.write(key, new IntWritable(i));
		}
		
	}
	    
		    
}
