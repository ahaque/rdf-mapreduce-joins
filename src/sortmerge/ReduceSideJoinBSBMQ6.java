package sortmerge;

/**
 * Reduce Side Join BSBM Q6
 * @date April 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReduceSideJoinBSBMQ6 {
	
	// Begin Query Information
	// Part of the label for: bsbm-inst_dataFromProducer105/Product4956
	private static String Word1 = "olympians";
	// End Query Information
			
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		// Zookeeper quorum is usually the same as the HBase master node
		String USAGE_MSG = "Arguments: <table name> <zookeeper quorum>";

		if (args == null || args.length != 2) {
			System.out.println("\n  You entered " + args.length + " arguments.");
			System.out.println("  " + USAGE_MSG);
			System.exit(0);
		}
		
		Configuration hConf = HBaseConfiguration.create(new Configuration());
	    hConf.set("hbase.zookeeper.quorum", args[1]);
	    hConf.set("scan.table", args[0]);
	    hConf.set("hbase.zookeeper.property.clientPort", "2181");
	    
		startJob_Stage1(args, hConf);
	}
	
	// MapReduce Stage-1 Job
	public static Job startJob_Stage1(String[] args, Configuration hConf) throws IOException {
		
		// args[0] = hbase table name
		// args[1] = zookeeper

		/*
		 * MapReduce Stage-1 Job
		 * Retrieve a list of subjects and their attributes
		 */
	    Scan scan1 = new Scan();		
		Job job1 = new Job(hConf);
		job1.setJobName("BSBM-Q6-ReduceSideJoin");
		job1.setJarByClass(ReduceSideJoinBSBMQ6.class);
		// Change caching and number of time stamps to speed up the scan
		scan1.setCaching(500);        
		scan1.setMaxVersions(200);
		scan1.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],		// input HBase table name
				scan1,			// Scan instance to control CF and attribute selection
				ReduceSideJoin_MapperStage1.class,	// mapper class
				Text.class,		// mapper output key
				KeyValueArrayWritable.class,  		// mapper output value
				job1);

		// Reducer settings
		job1.setReducerClass(SharedServices.ReduceSideJoin_Reducer.class);   
		job1.setOutputFormatClass(TextOutputFormat.class);
		//job1.setNumReduceTasks(1);  // Uncomment this if running into problems on 2+ node cluster
		FileOutputFormat.setOutputPath(job1, new Path("output/BSBMQ6"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}

	
	public static class ReduceSideJoin_MapperStage1 extends TableMapper<Text, KeyValueArrayWritable> {
		
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* BERLIN SPARQL BENHCMARK QUERY 6
		   ----------------------------------------
			SELECT ?product ?label
			WHERE {
				[TP-01] ?product rdfs:label ?label .
			 	[TP-02] ?product rdf:type bsbm:Product .
				[TP-03] FILTER regex(?label, "%word1%")
			}
		   ---------------------------------------
		 */
			String rowKey = new String(value.getRow());
			text.set(rowKey);
						
			// TP-02
			byte[] rawBytes = value.getValue(SharedServices.CF_AS_BYTES, "bsbm-voc_Product".getBytes());
			if (rawBytes == null) {
				return;
			}
			if (!(new String(rawBytes).equals("rdf_type"))) {
				return;
			}
			
			List<KeyValue> keyValuesToTransmit = new ArrayList<KeyValue>();
			for (KeyValue kv : value.list()) {
				if (new String(kv.getQualifier()).equals("rdfs_label")) {
					keyValuesToTransmit.add(kv);
				}
			}
			
			String productLabel = new String(keyValuesToTransmit.get(0).getValue());
			if (productLabel.contains(Word1)) {
				context.write(text, new KeyValueArrayWritable(SharedServices.listToArray(keyValuesToTransmit)));
			}
		}
	}
	
}
