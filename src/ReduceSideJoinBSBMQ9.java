/**
 * Reduce Side Join BSBM Q9
 * @date April 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReduceSideJoinBSBMQ9 {
	
	// Begin Query Information
	private static String ReviewXYZ = "bsbm-inst_dataFromRatingSite28/Review277734";
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
		job1.setJobName("BSBM-Q9-ReduceSideJoin");
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
		FileOutputFormat.setOutputPath(job1, new Path("output/BSBMQ9"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}

	
	public static class ReduceSideJoin_MapperStage1 extends TableMapper<Text, KeyValueArrayWritable> {
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* BERLIN SPARQL BENHCMARK QUERY 9
		   ----------------------------------------			
			DESCRIBE ?x
			WHERE {
				[TP-01] %ReviewXYZ% rev:reviewer ?x
			}
		   ---------------------------------------*/
			// TP-01
			String rowKey = new String(value.getRow());
			if (!rowKey.equals(ReviewXYZ)) {
				return;
			}
			// TP-01
			for (KeyValue kv : SharedServices.getKeyValuesContainingPredicate(value.list(), "rev_reviewer")) {
				List<KeyValue> oneReviewerKvs = new ArrayList<KeyValue>();
				oneReviewerKvs.add(kv);
				context.write(new Text(new String(kv.getRow())), new KeyValueArrayWritable(SharedServices.listToArray(oneReviewerKvs)));
			}
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class ReduceSideJoin_ReducerStage1 extends Reducer<Text, KeyValueArrayWritable, Text, Text>  {
		
	    HTable table;
	    Text productKey = new Text();

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	      Configuration conf = context.getConfiguration();
	      table = new HTable(conf, conf.get("scan.table"));
	    }

		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
			/* BERLIN SPARQL BENHCMARK QUERY 10
			   ----------------------------------------
			DESCRIBE ?x
			WHERE {
				[TP-01] %ReviewXYZ% rev:reviewer ?x
			} --------------------------------------- */
			
			List<KeyValue> finalKeyValues = new ArrayList<KeyValue>();
			// Get the unique reviewers
			HashSet<KeyValue> reviewerKvSet = new HashSet<KeyValue>();
			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (Arrays.equals(kv.getValue(),"rev_reviewer".getBytes())) {
						reviewerKvSet.add(kv);
					}
				}
			}
			// For each unique reviewer (should only be 1) get all the info about it
			for (KeyValue kv : reviewerKvSet) {
				Result reviewerResult = table.get(new Get(kv.getQualifier()));
				finalKeyValues.addAll(reviewerResult.list());
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
