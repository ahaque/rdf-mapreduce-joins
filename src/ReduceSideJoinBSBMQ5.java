/**
 * Reduce Side Join BSBM Q5
 * @date April 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReduceSideJoinBSBMQ5 {
	
	// Begin Query Information
	private static String ProductXYZ = "bsbm-inst_dataFromProducer105/Product4956";
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
		job1.setJobName("BSBM-Q5-ReduceSideJoin");
		job1.setJarByClass(ReduceSideJoinBSBMQ2.class);
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
		job1.setReducerClass(ReduceSideJoin_ReducerStage1.class);   
		job1.setOutputFormatClass(TextOutputFormat.class);
		//job1.setNumReduceTasks(1);  // Uncomment this if running into problems on 2+ node cluster
		FileOutputFormat.setOutputPath(job1, new Path("output/BSBMQ5"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}

	
	public static class ReduceSideJoin_MapperStage1 extends TableMapper<Text, KeyValueArrayWritable> {
		
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* BERLIN SPARQL BENHCMARK QUERY 5
		   ----------------------------------------
		SELECT DISTINCT ?product ?productLabel
		WHERE { 
			[TP-01] ?product rdfs:label ?productLabel .
			[TP-02] ?product bsbm:productFeature ?prodFeature .
		 	[TP-03] ?product bsbm:productPropertyNumeric2 ?simProperty2 .
		 	[TP-04] ?product bsbm:productPropertyNumeric1 ?simProperty1 .
			[TP-05] %ProductXYZ% bsbm:productFeature ?prodFeature .
			[TP-06] %ProductXYZ% bsbm:productPropertyNumeric1 ?origProperty1 .
			[TP-07] %ProductXYZ% bsbm:productPropertyNumeric2 ?origProperty2 .
			[TP-08] FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 – 120))
			[TP-09] FILTER (%ProductXYZ% != ?product)
			[TP-10] FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 – 170))
		}
		ORDER BY ?productLabel
		LIMIT 5
		   ---------------------------------------
		 */
			String rowKey = new String(value.getRow());
			text.set(rowKey);
			
			// TP-09
			if (rowKey.equals(ProductXYZ)) {
				return;
			}
			
			ArrayList<KeyValue> keyValuesToTransmit = new ArrayList<KeyValue>();
			List<KeyValue> productRow = value.list();
			// TP-02
			keyValuesToTransmit.addAll(SharedServices.getKeyValuesContainingPredicate(productRow, "bsbm-voc_productFeature"));
			
			boolean foundNumeric1 = false;
			boolean foundNumeric2 = false;
			
			for (KeyValue kv : productRow) {
				if (new String(kv.getQualifier()).equals("rdfs_label")) {
					keyValuesToTransmit.add(kv);
				}
				// TP-04
				if (new String(kv.getQualifier()).equals("bsbm-voc_productPropertyNumeric1")) {
					keyValuesToTransmit.add(kv);
					foundNumeric1 = true;
				}
				// TP-03
				else if (new String(kv.getQualifier()).equals("bsbm-voc_productPropertyNumeric2")) {
					keyValuesToTransmit.add(kv);
					foundNumeric2 = true;
				}
			}
			if (foundNumeric1 && foundNumeric2) {
				// Write the output product key-value
				context.write(text, new KeyValueArrayWritable(SharedServices.listToArray(keyValuesToTransmit)));
			}
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class ReduceSideJoin_ReducerStage1 extends Reducer<Text, KeyValueArrayWritable, Text, Text>  {
		
	    HTable table;

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	      Configuration conf = context.getConfiguration();
	      table = new HTable(conf, conf.get("scan.table"));
	    }

		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
			/* BERLIN SPARQL BENHCMARK QUERY 5
			   ----------------------------------------
			SELECT DISTINCT ?product ?productLabel
			WHERE { 
				[TP-01] ?product rdfs:label ?productLabel .
				[TP-02] ?product bsbm:productFeature ?prodFeature .
			 	[TP-03] ?product bsbm:productPropertyNumeric2 ?simProperty2 .
			 	[TP-04] ?product bsbm:productPropertyNumeric1 ?simProperty1 .
				[TP-05] %ProductXYZ% bsbm:productFeature ?prodFeature .
				[TP-06] %ProductXYZ% bsbm:productPropertyNumeric1 ?origProperty1 .
				[TP-07] %ProductXYZ% bsbm:productPropertyNumeric2 ?origProperty2 .
				[TP-08] FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 – 120))
				[TP-09] FILTER (%ProductXYZ% != ?product)
				[TP-10] FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 – 170))
			}
			ORDER BY ?productLabel
			LIMIT 5
			   ---------------------------------------
			 */
			StringBuilder builder = new StringBuilder();
			builder.append("\n");
			// Check to see if this product has a similar feature with ProductXYZ
			Get productXYZget = new Get(ProductXYZ.getBytes());
			Result productXYZresult = table.get(productXYZget);
			// Get list of ProductXYZ features
			List<KeyValue> productXYZasList = productXYZresult.list();
			List<KeyValue> productXYZkvs = SharedServices.getKeyValuesContainingPredicate(productXYZasList, "bsbm-voc_productFeature");
			
			// Extract features as strings
			List<String> productXYZfeatures = new ArrayList<String>();
			for (KeyValue kv : productXYZkvs) {
				productXYZfeatures.add(new String(kv.getQualifier()));;
			}
			// Make sure this product has at least one feature in common with ProductXYZ
			KeyValue simProperty1KeyValue = null;
			KeyValue simProperty2KeyValue = null;
			boolean featureMatch = false;
			
			for (KeyValueArrayWritable array : values) {
		        for (KeyValue kv : (KeyValue[]) array.toArray()) {
		        	if (featureMatch == false && new String(kv.getValue()).equals("bsbm-voc_productFeature".getBytes())) {
		        		if (productXYZfeatures.contains(new String(kv.getQualifier()))) {
		        			featureMatch = true;
		        		}
		        	}
		        	// Get the product label
		        	// Check the column name/qualifier since these are literals
		        	if (new String(kv.getQualifier()).equals("rdfs_label")) {
		        		builder.append(SharedServices.keyValueToString(kv));
			        	builder.append(SharedServices.SUBVALUE_DELIMITER);
			        	builder.append("\n");
		        	} else if (new String(kv.getQualifier()).equals("bsbm-voc_productPropertyNumeric1")) {
		        		simProperty1KeyValue = kv;
		        	} else if (new String(kv.getQualifier()).equals("bsbm-voc_productPropertyNumeric2")) {
		        		simProperty2KeyValue = kv;
		        	}
		        }
		    }
			if (featureMatch == false) {
				//return;
			}
			
			// Uncomment this to see the numeric values printed in the output file
//			builder.append(SharedServices.keyValueToString(simProperty1KeyValue));
//        	builder.append(SharedServices.SUBVALUE_DELIMITER);
//        	builder.append("\n");
//        	builder.append(SharedServices.keyValueToString(simProperty2KeyValue));
//        	builder.append(SharedServices.SUBVALUE_DELIMITER);
//        	builder.append("\n");
					
			// Check to see if the property numerics are similar
			// TP-06 and TP-07
			byte[] XYZnumeric1_bytes = productXYZresult.getValue(SharedServices.CF_AS_BYTES, "bsbm-voc_productPropertyNumeric1".getBytes());
			byte[] XYZnumeric2_bytes = productXYZresult.getValue(SharedServices.CF_AS_BYTES, "bsbm-voc_productPropertyNumeric2".getBytes());
			
			int origProperty1 = -1;
			int origProperty2 = -1;
			try {
				origProperty1 = ByteBuffer.wrap(XYZnumeric1_bytes).getInt();
			} catch (NumberFormatException e) {
				origProperty1 = -1;
			}
			try {
				origProperty2 = ByteBuffer.wrap(XYZnumeric2_bytes).getInt();
			} catch (NumberFormatException e) {
				origProperty2 = -1;
			}
			// Convert them to ints so we can compare
			int simProperty1 = -1;
			int simProperty2 = -1;
			try {
				simProperty1 = ByteBuffer.wrap(simProperty1KeyValue.getValue()).getInt();
			} catch (NumberFormatException e) {
				simProperty1 = -1;
			}
			try {
				simProperty2 = ByteBuffer.wrap(simProperty2KeyValue.getValue()).getInt();
			} catch (NumberFormatException e) {
				simProperty2 = -1;
			}
			/*
			 * 	[TP-08] FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 – 120))
				[TP-10] FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 – 170))
			 */
			if (!(simProperty1 < (origProperty1 + 120))) {
				return;
			}
			if (!(simProperty1 > (origProperty1 - 120))) {
				return;
			}
			if (!(simProperty2 < (origProperty2 + 170))) {
				return;
			}
			if (!(simProperty2 > (origProperty2 - 170))) {
				return;
			}
			// If the code has made it to this point, then we can output the data
			// Which is only the product (row key) and it's label (added to StringBuilder above)
			context.write(key, new Text(builder.toString()));
		}
	}	    
}
