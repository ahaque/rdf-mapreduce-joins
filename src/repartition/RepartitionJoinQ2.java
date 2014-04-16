package repartition;

/**
 * Reduce Side Join BSBM Q2
 * @date March 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import sortmerge.SharedServices;
import sortmerge.KeyValueArrayWritable;

public class RepartitionJoinQ2 {
	
	// Begin Query Information
	private static String ProductXYZ = "bsbm-inst_dataFromProducer105/Product4956";
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
	    
		startJob_Stage1(args);
	}
	
	// MapReduce Stage-1 Job
	public static Job startJob_Stage1(String[] args) throws IOException {
		
		// args[0] = hbase table name
		// args[1] = zookeeper

		Configuration hConf = HBaseConfiguration.create(new Configuration());
	    hConf.set("hbase.zookeeper.quorum", args[1]);
	    hConf.set("scan.table", args[0]);
	    hConf.set("hbase.zookeeper.property.clientPort", "2181");

	    Scan scan = new Scan();		
		Job job1 = new Job(hConf);
		job1.setJobName("BSBM-Q2-RepartitionJoin");
		job1.setJarByClass(RepartitionJoinQ2.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
		scan.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],        // input HBase table name
				scan,           // Scan instance to control CF and attribute selection
				RepartitionMapper.class,     // mapper
				CompositeKeyWritable.class,   // mapper output key
				KeyValueArrayWritable.class,  // mapper output value
				job1);
		
		// Repartition settings
		job1.setPartitionerClass(CompositePartitioner.class);
		job1.setSortComparatorClass(CompositeSortComparator.class);
		job1.setGroupingComparatorClass(CompositeGroupingComparator.class);
		
		// Reducer settings
		job1.setReducerClass(RepartitionReducer.class);
		//job1.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job1, new Path("output/BSBMQ2"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}

	
	public static class RepartitionMapper extends TableMapper<CompositeKeyWritable, KeyValueArrayWritable> {
		
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
			
			// PRODUCT
			// Convert to array list
			ArrayList<String> projectedVariablesList = new ArrayList<String>();
			for (String col : ProjectedVariables) {
				projectedVariablesList.add(col);
			}
			ArrayList<String> optionalVariablesList = new ArrayList<String>();
			for (String col : OptionalProjectedVariables) {
				optionalVariablesList.add(col);
			}
			
			// Add the join attributes
			// TP-08 and TP-09
			List<KeyValue> publisher = SharedServices.getKeyValuesContainingPredicate(entireRowAsList, "dc_publisher");
			if (publisher.size() == 0) {
				publisher = SharedServices.getKeyValuesContainingPredicate(entireRowAsList, "bsbm-voc_producer");
			}
			keyValuesToTransmit.addAll(publisher);
			// TP-11
			List<KeyValue> feature = SharedServices.getKeyValuesContainingPredicate(entireRowAsList, "bsbm-voc_productFeature");
			keyValuesToTransmit.addAll(feature);
			
			// Get the relevant columns from the table
			for (KeyValue kv : entireRowAsList) {
				String columnName = new String(kv.getQualifier());
				// Make sure this row contains all required columns
				for (int i = 0; i < projectedVariablesList.size(); i++) {
					if (columnName.equals(projectedVariablesList.get(i))
							|| new String(kv.getValue()).equals(projectedVariablesList.get(i))) {
						// Tag this keyvalue as "R2"
						keyValuesToTransmit.add(SharedServices.addTagToKv(kv, KeyValue.Type.Minimum));
						projectedVariablesList.remove(i);
						break;
					}
				}
				// Get any optional columns if they exist
				for (int i = 0; i < optionalVariablesList.size(); i++) {
					if (columnName.equals(optionalVariablesList.get(i))) {
						keyValuesToTransmit.add(SharedServices.addTagToKv(kv, KeyValue.Type.Minimum));
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
			context.write(new CompositeKeyWritable(rowKey,1), new KeyValueArrayWritable(SharedServices.listToArray(keyValuesToTransmit)));
		}
	}
	
	public static class RepartitionReducer extends Reducer<CompositeKeyWritable, KeyValueArrayWritable, Text, Text>  {
		
	    HTable table;

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	      Configuration conf = context.getConfiguration();
	      table = new HTable(conf, conf.get("scan.table"));
	    }

		public void reduce(CompositeKeyWritable key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {

			StringBuilder builder = new StringBuilder();
			byte[] publisherKey = null;
			builder.append("\n");
			ArrayList<byte[]> featureKeys = new ArrayList<byte[]>();
			for (KeyValueArrayWritable array : values) {
		        for (KeyValue kv : (KeyValue[]) array.toArray()) {
		        	builder.append(SharedServices.keyValueToString(kv));
		        	builder.append(SharedServices.SUBVALUE_DELIMITER);
		        	builder.append("\n");
		        	if(new String(kv.getValue()).equals("dc_publisher")) {
		        		publisherKey = kv.getQualifier();
		        	} else if(new String(kv.getValue()).equals("bsbm-voc_productFeature")) {
		        		featureKeys.add(kv.getQualifier());
		        	}
		        }
		      }
			if ((publisherKey == null)||(featureKeys.size() == 0)) {
				return;
			}
			// TP-10: For this product, get its producer's label
			Result publisher = table.get(new Get(publisherKey).addColumn(SharedServices.CF_AS_BYTES, "rdfs_label".getBytes()));
			builder.append(SharedServices.keyValueToString(publisher.list().get(0)));
			builder.append("\n");
			// TP-12: For this product, get its feature's label
			for (byte[] fkey : featureKeys) {
				Result feature = table.get(new Get(fkey).addColumn(SharedServices.CF_AS_BYTES, "rdfs_label".getBytes()));
				builder.append(SharedServices.keyValueToString(feature.list().get(0)));
				builder.append("\n");
			}
			
			context.write(new Text(key.getValue()), new Text(builder.toString()));
		}
	}	    
}
