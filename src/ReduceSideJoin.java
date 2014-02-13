import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import BSBMQuery7.Counters;

public class ReduceSideJoin {

	static String HBASE_TABLE_NAME = "rdf1";
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		
		// args[0] = zookeeper
		// args[1] = hbase table name
		
		Job job = run(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	@SuppressWarnings("deprecation")
	public static Job run(String[] args) throws IOException {
		
		Configuration hConf = HBaseConfiguration.create(new Configuration());
	    hConf.set("hbase.zookeeper.quorum", args[0]);
	    hConf.set("scan.table", args[1]);
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

		boolean b = job.waitForCompletion(true);
		if (!b) { throw new IOException("error with job!"); }
		
		return job;
	}
	
	public static class MyMapper extends TableMapper<Text, Text> {

		  public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		    // process data for the row from the Result instance.
		   }
		}
		    
}
