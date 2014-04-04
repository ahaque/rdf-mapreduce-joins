/**
 * Reduce Side Join BSBM Q1
 * @date March 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReduceSideJoinBSBMQ11 {
	
	// Begin Query Information
	private static String OfferXYZ = "";
	// End Query Information
		
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		// Zookeeper quorum is usually the same as the HBase master node
		String USAGE_MSG = "Arguments: <table name> <zookeeper quorum>";

		if (args == null || args.length != 2) {
			System.out.println("\n  You entered " + args.length + " arguments.");
			System.out.println("  " + USAGE_MSG);
			System.exit(0);
		}
		startJob(args);
	}
	
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
		job.setJobName("BSBM-Q11-ReduceSideJoin");
		job.setJarByClass(ReduceSideJoinBSBMQ11.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
		scan.setMaxVersions(200);
		scan.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],        // input HBase table name
				scan,             // Scan instance to control CF and attribute selection
				ReduceSideJoin_Mapper.class,   // mapper
				Text.class,         // mapper output key
				KeyValueArrayWritable.class,  // mapper output value
				job);

		// Reducer settings
		job.setReducerClass(SharedServices.ReduceSideJoin_Reducer.class);    // reducer class
		job.setNumReduceTasks(1);    // at least one, adjust as required
	
		FileOutputFormat.setOutputPath(job, new Path("output/BSBMQ11"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class ReduceSideJoin_Mapper extends TableMapper<Text, KeyValueArrayWritable> {
		
		private Text text = new Text();
		
		private boolean isPartOfFirstUnion(Result value) {
			if (!(new String(value.getRow()).equals(OfferXYZ))) {
				return false;
			}
			return true;
		}

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			text.set(new String(value.getRow()));
		/* BERLIN SPARQL BENHCMARK QUERY 11
		   ----------------------------------------
			SELECT ?property ?hasValue ?isValueOf
			WHERE {
			[TP-01]	{ %OfferXYZ% ?property ?hasValue }
					UNION
			[TP-02]	{ ?isValueOf ?property %OfferXYZ% }
			}
		   ---------------------------------------
		 */
			// TP-01
			if (isPartOfFirstUnion(value)) {
				List<KeyValue> entireRowAsList = value.list();
		    	context.write(text, new KeyValueArrayWritable((KeyValue[]) entireRowAsList.toArray()));
		    	return;
			}
			// TP-02
			else {
				List<KeyValue> entireRowAsList = value.list();
				// Check all cells and see if the OFFER is part of the value
				for (KeyValue kv : entireRowAsList) {
					if (!(new String(kv.getValue()).equals(OfferXYZ))) {
						entireRowAsList.remove(kv);
					}
				}
		    	context.write(text, new KeyValueArrayWritable((KeyValue[]) entireRowAsList.toArray()));
			}
		}
	}	    
}
