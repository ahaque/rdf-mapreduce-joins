import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class ReduceSideJoin {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
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
				  tableName,        // input HBase table name
				  scan,             // Scan instance to control CF and attribute selection
				  MyMapper.class,   // mapper
				  null,             // mapper output key
				  null,             // mapper output value
				  job);
				job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper

				boolean b = job.waitForCompletion(true);
				if (!b) {
				  throw new IOException("error with job!");
		
		return job;
	}
	
}
