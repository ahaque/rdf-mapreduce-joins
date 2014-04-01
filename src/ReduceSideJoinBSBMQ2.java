/**
 * Reduce Side Join BSBM Q2
 * @date March 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
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
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReduceSideJoinBSBMQ2 {
	
	// Begin Query Information
	private static String ProductXYZ = "bsbm-inst_dataFromProducer460/Product22747";
	private static String[] ProjectedVariables = {
		"rdfs_label",
		"rdfs_comment",
		"bsbm-voc_productFeature",
		"bsbm-voc_productPropertyTextual1",
		"bsbm-voc_productPropertyTextual2",
		"bsbm-voc_productPropertyTextual3",
		"bsbm-voc_productPropertyNumeric1",
		"bsbm-voc_productPropertyNumeric2"
		};
	private static String[] OptionalProjectedVariables = {
		"bsbm-voc_productPropertyTextual4",
		"bsbm-voc_productPropertyTextual5",
		"bsbm-voc_productPropertyNumeric4"
	};
	// End Query Information
	
	private static byte[] CF_AS_BYTES = "o".getBytes();
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		// Zookeeper quorum is usually the same as the HBase master node
		String USAGE_MSG = "Arguments: <table name> <zookeeper quorum>";

		if (args == null || args.length != 2) {
			System.out.println("\n  You entered " + args.length + " arguments.");
			System.out.println("  " + USAGE_MSG);
			System.exit(0);
		}
				
		startJob_Stage1(args);
		startJob_Stage2(args);
	}
	
	public static Job startJob_Stage1(String[] args) throws IOException {
		
		// args[0] = hbase table name
		// args[1] = zookeeper
		
		Configuration hConf = HBaseConfiguration.create(new Configuration());
	    hConf.set("hbase.zookeeper.quorum", args[1]);
	    hConf.set("scan.table", args[0]);
	    hConf.set("hbase.zookeeper.property.clientPort", "2181");

		/*
		 * MapReduce Stage-1 Job
		 * Retreive a list of subjects and their attributes
		 */
	    Scan scan1 = new Scan();		
		Job job1 = new Job(hConf);
		job1.setJobName("BSBM-Q2-ReduceSideJoin-Stage1");
		job1.setJarByClass(ReduceSideJoinBSBMQ1.class);
		// Change caching to speed up the scan
		scan1.setCaching(500);        
		scan1.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],        // input HBase table name
				scan1,             // Scan instance to control CF and attribute selection
				ReduceSideJoin_MapperStage1.class,   // mapper
				Text.class,         // mapper output key
				KeyValueArrayWritable.class,  // mapper output value
				job1);

		// Reducer settings
		job1.setReducerClass(ReduceSideJoin_ReducerStage1.class);    // reducer class
		job1.setNumReduceTasks(1);    // at least one, adjust as required
		FileOutputFormat.setOutputPath(job1, new Path("output/BSBMQ2/Stage1"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}

	/*
	 * MapReduce Stage-2 Job
	 * Pull attributes for nodes one degree from subject from Stage-1
	 */
public static Job startJob_Stage2(String[] args) throws IOException {
		
		// args[0] = hbase table name
		// args[1] = zookeeper
		
		Configuration hConf = HBaseConfiguration.create(new Configuration());
	    hConf.set("hbase.zookeeper.quorum", args[1]);
	    hConf.set("scan.table", args[0]);
	    hConf.set("hbase.zookeeper.property.clientPort", "2181");

		/*
		 * MapReduce Stage-1 Job
		 * Retreive a list of subjects and their attributes
		 */
	    Scan scan2 = new Scan();		
		Job job2 = new Job(hConf);
		job2.setJobName("BSBM-Q2-ReduceSideJoin-Stage2");
		job2.setJarByClass(ReduceSideJoinBSBMQ1.class);
		// Change caching to speed up the scan
		scan2.setCaching(500);        
		scan2.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],        // input HBase table name
				scan2,             // Scan instance to control CF and attribute selection
				ReduceSideJoin_MapperStage1.class,   // mapper
				Text.class,         // mapper output key
				KeyValueArrayWritable.class,  // mapper output value
				job2);

		// Reducer settings
		job2.setReducerClass(ReduceSideJoin_ReducerStage2.class);    // reducer class
		job2.setNumReduceTasks(1);    // at least one, adjust as required

		FileOutputFormat.setOutputPath(job2, new Path("output/BSBMQ2/Stage2"));

		try {
			job2.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job2;
	}
 
	
	public static class ReduceSideJoin_MapperStage1 extends TableMapper<Text, KeyValueArrayWritable> {
		
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* BERLIN SPARQL BENHCMARK QUERY 2
		   ----------------------------------------
SELECT
	?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3
 	?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4 
WHERE {
	[TriplePattern-01]	%ProductXYZ% rdfs:label ?label .
	[TriplePattern-02]	%ProductXYZ% rdfs:comment ?comment .
	[TriplePattern-03]	%ProductXYZ% bsbm:productPropertyTextual1 ?propertyTextual1 .
	[TriplePattern-04]	%ProductXYZ% bsbm:productPropertyTextual2 ?propertyTextual2 .
	[TriplePattern-05]	%ProductXYZ% bsbm:productPropertyTextual3 ?propertyTextual3 .
	[TriplePattern-06]	%ProductXYZ% bsbm:productPropertyNumeric1 ?propertyNumeric1 .
	[TriplePattern-07]	%ProductXYZ% bsbm:productPropertyNumeric2 ?propertyNumeric2 .
	[TriplePattern-08]	%ProductXYZ% dc:publisher ?p . 
	[TriplePattern-09]	%ProductXYZ% bsbm:producer ?p .
	[TriplePattern-10]	?p rdfs:label ?producer .
	[TriplePattern-11]	%ProductXYZ% bsbm:productFeature ?f .
	[TriplePattern-12]	?f rdfs:label ?productFeature .
	[TriplePattern-13]	OPTIONAL { %ProductXYZ% bsbm:productPropertyTextual4 ?propertyTextual4 }
	[TriplePattern-14]	OPTIONAL { %ProductXYZ% bsbm:productPropertyTextual5 ?propertyTextual5 }
	[TriplePattern-15]	OPTIONAL { %ProductXYZ% bsbm:productPropertyNumeric4 ?propertyNumeric4 }
}
		   ---------------------------------------
		 */
			String rowKey = new String(value.getRow());
			if (!rowKey.equals(ProductXYZ)) {
				return;
			}
			List<KeyValue> entireRowAsList = value.list();
			ArrayList<KeyValue> keyValuesToTransmit = new ArrayList<KeyValue>();

			text.set(rowKey);

			// PRODUCER
			// TriplePattern-10
			ArrayList<KeyValue> publisherRowList = new ArrayList<KeyValue>();
			// Add the "table" tags
			for (KeyValue kv : entireRowAsList) {
				if (new String(kv.getQualifier()).equals("rdfs_label")) {
					// Tag this KeyValue as R1
					publisherRowList.add(SharedServices.addTagToKv(kv, KeyValue.Type.Maximum));
				}
			}
			// If row doesn't have a label, then we don't emit anything
			// If row has "rdfs_label" then we output it
			if (publisherRowList.size() != 0) {
				keyValuesToTransmit.addAll(publisherRowList);
			}
			
			// PRODUCT
			List<KeyValue> productRowList = new LinkedList<KeyValue>();
			// Convert to array list
			ArrayList<String> projectedVariablesList = new ArrayList<String>();
			for (String col : ProjectedVariables) {
				projectedVariablesList.add(col);
			}
			ArrayList<String> optionalVariablesList = new ArrayList<String>();
			for (String col : OptionalProjectedVariables) {
				optionalVariablesList.add(col);
			}
			
			// Get the relevant columns from the table
			for (KeyValue kv : entireRowAsList) {
				String columnName = new String(kv.getQualifier());
				// Make sure this row contains all required columns
				for (int i = 0; i < projectedVariablesList.size(); i++) {
					if (columnName.equals(projectedVariablesList.get(i))
							|| new String(kv.getValue()).equals(projectedVariablesList.get(i))) {
						// Tag this keyvalue as "R2"
						productRowList.add(SharedServices.addTagToKv(kv, KeyValue.Type.Minimum));
						projectedVariablesList.remove(i);
						break;
					}
				}
				// Get any optional columns if they exist
				for (int i = 0; i < optionalVariablesList.size(); i++) {
					if (columnName.equals(optionalVariablesList.get(i))) {
						productRowList.add(SharedServices.addTagToKv(kv, KeyValue.Type.Minimum));
						optionalVariablesList.remove(i);
						break;
					}
				}
			}
			// If the row is missing a required variable, we're done here, go to next row
			if (projectedVariablesList.size() > 0) {
				return;
			}
			// Write the output product key-value
			keyValuesToTransmit.addAll(productRowList);
			context.write(text, new KeyValueArrayWritable(SharedServices.listToArray(keyValuesToTransmit)));
		}
	}
	
	public static class ReduceSideJoin_ReducerStage1 extends Reducer<Text, KeyValueArrayWritable, Text, Text>  {
		
		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
		      StringBuilder builder = new StringBuilder();
		      for (KeyValueArrayWritable array : values) {
		    	builder.append("\n");
		        for (KeyValue kv : (KeyValue[]) array.toArray()) {
		        	String[] triple = null;
		        	try {
						triple = SharedServices.keyValueToTripleString(kv);
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
		        	builder.append("\t" + triple[1] + "\t" + triple[2] +"\n");
		        }
		      }
			context.write(key, new Text(builder.toString()));
		}
	}
	
	public static class ReduceSideJoin_ReducerStage2 extends Reducer<Text, KeyValueArrayWritable, Text, Text>  {
		
		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
		      StringBuilder builder = new StringBuilder();
		      for (KeyValueArrayWritable array : values) {
		    	builder.append("\n");
		        for (KeyValue kv : (KeyValue[]) array.toArray()) {
		        	String[] triple = null;
		        	try {
						triple = SharedServices.keyValueToTripleString(kv);
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
		        	builder.append("\t" + triple[1] + "\t" + triple[2] +"\n");
		        }
		      }
			context.write(key, new Text(builder.toString()));
		}
	}
		    
}
