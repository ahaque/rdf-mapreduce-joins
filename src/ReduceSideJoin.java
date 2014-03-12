import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReduceSideJoin {

	static String HBASE_TABLE_NAME = "rdf1";
	
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
		
		Job job = new Job(hConf);
		job.setJobName("HBaseScan");
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
		
		FileOutputFormat.setOutputPath(job, new Path("data/2014-13-02_experiment.txt"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	public static class ReduceSideJoin_Mapper extends TableMapper<Text, IntWritable> {
		
		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			String val = new String(value.getValue("p".getBytes(), "bsbm_producer".getBytes()));
			text.set(val);
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
