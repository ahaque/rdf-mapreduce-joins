package lubm.sortmerge;

/**
 * Sort Merge Join LUBM Q9
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import bsbm.sortmerge.KeyValueArrayWritable;
import bsbm.sortmerge.SharedServices;


public class SortMergeLUBMQ9 {
		
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
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
		job.setJobName("LUBM-Q9-SortMerge");
		job.setJarByClass(SortMergeLUBMQ9.class);
		scan.setCaching(500);        
		scan.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],        // input HBase table name
				scan,             // Scan instance to control CF and attribute selection
				SortMergeMapper.class,   // mapper
				Text.class,         // mapper output key
				KeyValueArrayWritable.class,  // mapper output value
				job);

		// Reducer settings
		job.setReducerClass(SortMergeReducer.class);    // reducer class
		//job.setReducerClass(SharedServices.ReduceSideJoin_Reducer.class);
		//job.setNumReduceTasks(1);    // at least one, adjust as required
	
		FileOutputFormat.setOutputPath(job, new Path("output/LUBMQ9"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class SortMergeMapper extends TableMapper<Text, KeyValueArrayWritable> {
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* LUBM QUERY 9
		   ----------------------------------------
		SELECT ?X, ?Y, ?Z
		WHERE
		{ [TP-01] ?X rdf:type ub:Student .
		  [TP-02] ?Y rdf:type ub:Faculty .
		  [TP-03] ?Z rdf:type ub:Course .
		  [TP-04] ?X ub:advisor ?Y .
		  [TP-05] ?Y ub:teacherOf ?Z .
		  [TP-06] ?X ub:takesCourse ?Z}
		   ---------------------------------------
		 */
	
			List<KeyValue> toTransmit = new ArrayList<KeyValue>();
			for (KeyValue kv : value.list()) {
				if (Arrays.equals(kv.getQualifier(), "ub_Student".getBytes())) {
					// TP-01
					if (!Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
						return;
					}
				} else if (Arrays.equals(kv.getValue(), "ub_memberOf".getBytes())) {
					// TP-03
					toTransmit.add(kv);
				} else if (Arrays.equals(kv.getQualifier(), "ub_emailAddress".getBytes())) {
					// TP-05
					toTransmit.add(kv);
				}
			}
			context.write(new Text(value.getRow()), new KeyValueArrayWritable(SharedServices.listToArray(toTransmit)));
		}
	}
	
	public static class SortMergeReducer extends Reducer<Text, KeyValueArrayWritable, Text, Text> {

		HTable table;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			table = new HTable(conf, conf.get("scan.table"));
		}

		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
			/* LUBM QUERY 9
			   ----------------------------------------
		SELECT ?X, ?Y, ?Z
		WHERE
		{ [TP-01] ?X rdf:type ub:Student .
		  [TP-02] ?Y rdf:type ub:Faculty .
		  [TP-03] ?Z rdf:type ub:Course .
		  [TP-04] ?X ub:advisor ?Y .
		  [TP-05] ?Y ub:teacherOf ?Z .
		  [TP-06] ?X ub:takesCourse ?Z }
			   ---------------------------------------
			 */
			// Find the department name
			KeyValue kv_department = null;
			String email = null;
			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (Arrays.equals(kv.getValue(), "ub_memberOf".getBytes())) {
						kv_department = kv;
					} else if (Arrays.equals(kv.getQualifier(), "ub_emailAddress".getBytes())) {
						email = new String(kv.getValue());
					} 
				}
			}
			if (kv_department == null) {
				return;
			}

			Get g = new Get(kv_department.getQualifier());
			g.addColumn(SharedServices.CF_AS_BYTES, "ub_Department".getBytes());
			g.addColumn(SharedServices.CF_AS_BYTES, "University0.edu".getBytes());
			Result departmentResult = table.get(g);
			// TP-04
			byte[] subOrg = departmentResult.getValue(SharedServices.CF_AS_BYTES,"University0.edu".getBytes());
			if (subOrg == null) { return; }
			if (!Arrays.equals(subOrg, "ub_subOrganizationOf".getBytes())) { return; }
			// TP-02
			byte[] predType = departmentResult.getValue(SharedServices.CF_AS_BYTES,"ub_Department".getBytes());
			if (predType == null) {	return;	}
			if (!Arrays.equals(predType, "rdf_type".getBytes())) { return; }
			
			// If we've made it this far, then output the result
			StringBuilder builder = new StringBuilder();
			builder.append("\n");
			builder.append(new String(kv_department.getRow()) + "\t");
			builder.append(new String(kv_department.getQualifier()) + "\t");
			builder.append(email + "\n");
			context.write(key, new Text(builder.toString()));
		}
	}
}
