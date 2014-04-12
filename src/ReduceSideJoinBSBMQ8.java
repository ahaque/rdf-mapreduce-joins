/**
 * Reduce Side Join BSBM Q8
 * @date April 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class ReduceSideJoinBSBMQ8 {
	
	// Begin Query Information
	private static String ProductXYZ = "bsbm-inst_dataFromProducer284/Product13895";
	// End Query Information
			
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

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
		job1.setJobName("BSBM-Q8-ReduceSideJoin");
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
		//job1.setReducerClass(SharedServices.ReduceSideJoin_Reducer.class);   
		
		job1.setOutputFormatClass(TextOutputFormat.class);
		//job1.setNumReduceTasks(1);  // Uncomment this if running into problems on 2+ node cluster
		FileOutputFormat.setOutputPath(job1, new Path("output/BSBMQ8"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}

	
	public static class ReduceSideJoin_MapperStage1 extends TableMapper<Text, KeyValueArrayWritable> {
		
		private Text text = new Text();
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* BERLIN SPARQL BENHCMARK QUERY 10
		   ----------------------------------------
			SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 ?rating2 ?rating3 ?rating4 
			WHERE { 
				[TriplePattern-01] ?review bsbm:reviewFor %ProductXYZ% .
				[TP-02] ?review dc:title ?title .
				[TP-03] ?review rev:text ?text .
				[TP-04] FILTER langMatches( lang(?text), "EN" ) 
				[TP-05] ?review bsbm:reviewDate ?reviewDate .
				[TP-06] ?review rev:reviewer ?reviewer .
				[TP-07] ?reviewer foaf:name ?reviewerName .
				[TP-08] OPTIONAL { ?review bsbm:rating1 ?rating1 . }
				[TP-09] OPTIONAL { ?review bsbm:rating2 ?rating2 . }
				[TP-10] OPTIONAL { ?review bsbm:rating3 ?rating3 . }
				[TP-11] OPTIONAL { ?review bsbm:rating4 ?rating4 . }
			}
			ORDER BY DESC(?reviewDate)
			LIMIT 20
		   ---------------------------------------*/
			String rowKey = new String(value.getRow());
			text.set(rowKey);
			
			ArrayList<KeyValue> keyValuesToTransmit = new ArrayList<KeyValue>();
			List<KeyValue> reviewRow = value.list();
			byte[] predicate = value.getValue(SharedServices.CF_AS_BYTES, ProductXYZ.getBytes());
			if (!Arrays.equals(predicate, "bsbm-voc_reviewFor".getBytes())) {
				return;
			}
			
			int requiredColumns = 0;
			for (KeyValue kv : reviewRow) {
				// TP-01
				if (Arrays.equals(kv.getValue(), "bsbm-voc_reviewFor".getBytes())) {
					keyValuesToTransmit.add(kv);
					requiredColumns++;
				}
				// TP-02
				else if (Arrays.equals(kv.getQualifier(), "dc_title".getBytes())) {
					keyValuesToTransmit.add(kv);
					requiredColumns++;
				}
				// TP-03
				else if (Arrays.equals(kv.getQualifier(), "rev_text".getBytes())) {
					keyValuesToTransmit.add(kv);
					requiredColumns++;
				}
				// TP-04
				else if (Arrays.equals(kv.getValue(), "rdfs_lang".getBytes())) {
					if (!Arrays.equals(kv.getQualifier(), "@en".getBytes())) {
						return;
					}
					keyValuesToTransmit.add(kv);
					requiredColumns++;
				}
				// TP-05
				else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_reviewDate".getBytes())) {
					keyValuesToTransmit.add(kv);
					requiredColumns++;
				}
				// TP-06
				else if (Arrays.equals(kv.getValue(), "rev_reviewer".getBytes())) {
					keyValuesToTransmit.add(kv);
					requiredColumns++;
				}
				// OPTIONAL TP-08, TP-09, TP-10, TP-11
				else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_rating1".getBytes())) {
					keyValuesToTransmit.add(kv);
				}
				else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_rating2".getBytes())) {
					keyValuesToTransmit.add(kv);
				}
				else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_rating3".getBytes())) {
					keyValuesToTransmit.add(kv);
				}
				else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_rating4".getBytes())) {
					keyValuesToTransmit.add(kv);
				}
			}
			if (requiredColumns < 6) {
				return;
			}
			context.write(text, new KeyValueArrayWritable(SharedServices.listToArray(keyValuesToTransmit)));
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
			/* BERLIN SPARQL BENHCMARK QUERY 10
			   ----------------------------------------
			SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 ?rating2 ?rating3 ?rating4 
			WHERE { 
				[TP-01] ?review bsbm:reviewFor %ProductXYZ% .
				[TP-02] ?review dc:title ?title .
				[TP-03] ?review rev:text ?text .
				[TP-04] FILTER langMatches( lang(?text), "EN" ) 
				[TP-05] ?review bsbm:reviewDate ?reviewDate .
				[TP-06] ?review rev:reviewer ?reviewer .
				[TP-07] ?reviewer foaf:name ?reviewerName .
				[TP-08] OPTIONAL { ?review bsbm:rating1 ?rating1 . }
				[TP-09] OPTIONAL { ?review bsbm:rating2 ?rating2 . }
				[TP-10] OPTIONAL { ?review bsbm:rating3 ?rating3 . }
				[TP-11] OPTIONAL { ?review bsbm:rating4 ?rating4 . }
			}
			ORDER BY DESC(?reviewDate)
			LIMIT 20
			   --------------------------------------- */
			
			List<KeyValue> finalKeyValues = new ArrayList<KeyValue>();
			
			// Find the keys for the vendor/publisher
			KeyValue kv_reviewer = null;
			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (Arrays.equals(kv.getValue(), "rev_reviewer".getBytes())) {
						kv_reviewer = kv;
						finalKeyValues.add(kv);
					} else {
						finalKeyValues.add(kv);
					}
				}
			}
			// TP-07
			Result reviewerResult = table.get(new Get(kv_reviewer.getQualifier()));
			boolean foundReviewerName = false;
			for (KeyValue kv : reviewerResult.list()) {
				if (Arrays.equals(kv.getQualifier(), "foaf_name".getBytes())) {
					finalKeyValues.add(kv);
					foundReviewerName = true;
					break;
				}
			}
			if (foundReviewerName == false) {
				return;
			}
			
			// Format and output the values
			StringBuilder builder = new StringBuilder();
	    	builder.append("\n");
	        for (KeyValue kv : finalKeyValues) {
	        	String[] triple = null;
	        	try {
					triple = SharedServices.keyValueToTripleString(kv);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
	        	builder.append(triple[0] + "\t" + triple[1] + "\t" + triple[2] +"\n");
	        }

			context.write(key, new Text(builder.toString()));
		}
	}	    
}