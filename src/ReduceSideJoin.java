import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;


public class ReduceSideJoin {

	static String HBASE_TABLE_NAME = "rdf1";
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		
		// args[0] = zookeeper
		// args[1] = hbase table name
		
		String USAGE_MSG = "  Arguments: <zk quorum>";

		if (args == null || args.length != 1) {
			System.out.println(USAGE_MSG);
			System.exit(0);
		}

		Job job = startJob(args);
	}
	
	@SuppressWarnings("deprecation")
	public static Job startJob(String[] args) throws IOException {
		
		Configuration hConf = HBaseConfiguration.create(new Configuration());
	    hConf.set("hbase.zookeeper.quorum", args[0]);
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
		job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	public static class MyMapper extends TableMapper<Text, Text> {

		  public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		    // process data for the row from the Result instance.
		   }
		}
		    
}
