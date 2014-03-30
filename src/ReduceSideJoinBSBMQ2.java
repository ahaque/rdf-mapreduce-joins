/**
 * Reduce Side Join BSBM Q2
 * @date March 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class ReduceSideJoinBSBMQ2 {
	
	// Begin Query Information
	private static String ProductXYZ = "bsbm-inst_ProductType151";
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
		job.setJobName("BSBM-Q2-ReduceSideJoin");
		job.setJarByClass(ReduceSideJoinBSBMQ1.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
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
		job.setReducerClass(ReduceSideJoin_Reducer.class);    // reducer class
		job.setNumReduceTasks(1);    // at least one, adjust as required
	
		FileOutputFormat.setOutputPath(job, new Path("output/BSBMQ2"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class ReduceSideJoin_Mapper extends TableMapper<Text, KeyValueArrayWritable> {
		
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
			List<KeyValue> entireRowAsList = value.list();
			// Current row we're scanning = r
			// Set "text" to equal the row key of r
			text.set(new String(value.getRow()));

			// PRODUCER
			// TriplePattern-10
			ArrayList<KeyValue> publisherRowList = new ArrayList<KeyValue>();
			for (KeyValue kv : entireRowAsList) {
				if (new String(kv.getQualifier()).equals("rdfs_label")) {
					publisherRowList.add(SharedServices.addTagToKv(kv, KeyValue.Type.Minimum));
				}
			}
			// Convert to serializable format
			context.write(text, new KeyValueArrayWritable(SharedServices.listToArray(publisherRowList)));
			
			
//			// PRODUCT
//			List<KeyValue> productRowList = new LinkedList<KeyValue>();
//			// Convert to array list
//			ArrayList<String> projectedVariablesList = new ArrayList<String>();
//			for (String col : ProjectedVariables) {
//				projectedVariablesList.add(col);
//			}
//			// Make sure this row contains all required columns
//			for (KeyValue kv : entireRowAsList) {
//				for (String col : projectedVariablesList) {
//					if (new String(kv.getQualifier()).equals(col)) {
//						productRowList.add(addTagToKv(kv, KeyValue.Type.Maximum));
//					}
//				}
//				projectedVariablesList.remove(new String(kv.getValue()));
//			}
//			// If there is a required projected variable that doesn't exist, quit
//			if (projectedVariablesList.size() > 0) {
//				return;
//			}
//			KeyValue[] productRowListSerializable = new KeyValue[productRowList.size()];
//			for (int i = 0; i < productRowList.size(); i++) {
//				productRowListSerializable[i] = productRowList.get(i);
//			}
//			// Write the output product key-value
//			context.write(text, new KeyValueArrayWritable(productRowListSerializable));
	    	
		}
	}
	
	public static class ReduceSideJoin_Reducer extends Reducer<Text, KeyValueArrayWritable, Text, Text>  {
		
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
