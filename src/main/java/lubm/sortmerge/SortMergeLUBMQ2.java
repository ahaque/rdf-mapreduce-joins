package main.java.lubm.sortmerge;

/**
 * Sort Merge Join LUBM Q2
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import main.java.bsbm.sortmerge.ReduceSideJoinBSBMQ12;
import main.java.bsbm.sortmerge.ReduceSideJoinBSBMQ12.ReduceSideJoin_MapperStage1;
import main.java.bsbm.sortmerge.ReduceSideJoinBSBMQ12.ReduceSideJoin_ReducerStage1;

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

import main.java.bsbm.sortmerge.KeyValueArrayWritable;
import main.java.bsbm.sortmerge.SharedServices;


public class SortMergeLUBMQ2 {
	
	// Begin Query Information
	
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
	
	public static Job startJob_Stage1(String[] args, Configuration hConf) throws IOException {
		
	    Scan scan1 = new Scan();		
		Job job1 = new Job(hConf);
		job1.setJobName("LUBM-Q2-SortMerge-Stage1");
		job1.setJarByClass(ReduceSideJoinBSBMQ12.class);
		// Change caching and number of time stamps to speed up the scan
		scan1.setCaching(500);        
		scan1.setMaxVersions(200);
		scan1.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],		// input HBase table name
				scan1,			// Scan instance to control CF and attribute selection
				Stage1_SortMergeMapper.class,	// mapper class
				Text.class,		// mapper output key
				KeyValueArrayWritable.class,  		// mapper output value
				job1);

		// Reducer settings
		job1.setReducerClass(Stage1_SortMergeReducer.class);   
		job1.setOutputFormatClass(TextOutputFormat.class);
		//job1.setNumReduceTasks(1);  // Uncomment this if running into problems on 2+ node cluster
		FileOutputFormat.setOutputPath(job1, new Path("output/LUBMQ2"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}
	
	
	
	public static class Stage1_SortMergeMapper extends TableMapper<Text, KeyValueArrayWritable> {
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* LUBM QUERY 2
		   ----------------------------------------
		PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
		PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
		SELECT ?X, ?Y, ?Z
		WHERE
		{ [TP-01] ?X rdf:type ub:GraduateStudent .
		  [TP-02] ?Y rdf:type ub:University .
		  [TP-03] ?Z rdf:type ub:Department .
		  [TP-04] ?X ub:memberOf ?Z .
		  [TP-05] ?Z ub:subOrganizationOf ?Y .
		  [TP-06] ?X ub:undergraduateDegreeFrom ?Y}
		   ---------------------------------------
		 */
			// Determine if this row is a grad student, university, or department
			
			// TP-01, TP-02, TP-03
			byte[] x_type = value.getValue(SharedServices.CF_AS_BYTES, Bytes.toBytes("ub_GraduateStudent"));
			byte[] y_type = value.getValue(SharedServices.CF_AS_BYTES, Bytes.toBytes("ub_GraduateStudent"));
			byte[] z_type = value.getValue(SharedServices.CF_AS_BYTES, Bytes.toBytes("ub_GraduateStudent"));
			if (type1 == null) { return; }
			String type_string = new String(type1);
			if (!type_string.equals("rdf_type")) { return; }
			
			
			// HBase row for that subject (Mapper Output: Value)
			List<KeyValue> entireRowAsList = value.list();
			List<KeyValue> toTransmitList = new ArrayList<KeyValue>();
			
			// Get the KVs to transmit
			for (int i = 0; i < entireRowAsList.size(); i++) {
				// Reduce data sent across network by writing only columns that we know will be used
				if (Arrays.equals(entireRowAsList.get(i).getValue(), "ub_memberOf".getBytes())) {
					toTransmitList.add(entireRowAsList.get(i));
				} else if (Arrays.equals(entireRowAsList.get(i).getValue(), "ub_undergraduateDegreeFrom".getBytes())) {
					toTransmitList.add(entireRowAsList.get(i));
				}
			}
	    	context.write(new Text(value.getRow()), new KeyValueArrayWritable(SharedServices.listToArray(toTransmitList)));
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class Stage1_SortMergeReducer extends Reducer<Text, KeyValueArrayWritable, Text, Text> {

		HTable table;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			table = new HTable(conf, conf.get("scan.table"));
		}

		public void reduce(Text key, Iterable<KeyValueArrayWritable> values,
				Context context) throws IOException, InterruptedException {
			/* LUBM QUERY 2
			   ----------------------------------------
			SELECT ?X, ?Y, ?Z
			WHERE
			{ [TP-01] ?X rdf:type ub:GraduateStudent .
			  [TP-02] ?Y rdf:type ub:University .
			  [TP-03] ?Z rdf:type ub:Department .
			  [TP-04] ?X ub:memberOf ?Z .
			  [TP-05] ?Z ub:subOrganizationOf ?Y .
			  [TP-06] ?X ub:undergraduateDegreeFrom ?Y}
			   ---------------------------------------
			 */

			List<KeyValue> finalKeyValues = new ArrayList<KeyValue>();

			// Find and get university information
			// TP-06
			KeyValue kv_university = null;
			KeyValue kv_department = null;
			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (Arrays.equals(kv.getValue(), "ub_undergraduateDegreeFrom".getBytes())) {
						kv_university = kv;
						finalKeyValues.add(kv);
					} else if (Arrays.equals(kv.getValue(), "ub_memberOf".getBytes())) {
						kv_department = kv;
						finalKeyValues.add(kv);
					}
				}
			}
			if (kv_university == null) {
				return;
			}
			// TP-02
			Get g = new Get(kv_university.getQualifier());
			g.addColumn(SharedServices.CF_AS_BYTES, "ub_University".getBytes());
			Result universityResult = table.get(g);
			byte[] universityPredicate = universityResult.getValue(SharedServices.CF_AS_BYTES,"ub_University".getBytes());
			if (!Arrays.equals(universityPredicate, "rdf_type".getBytes())) {
				return;
			}
			
			// Find and get department information
			// TP-04
			if (kv_department == null) {
				return;
			}
			// TP-03
			Result departmentResult = table.get(new Get(kv_department.getQualifier()));
			// TP-05
			for (KeyValue kv : departmentResult.list()) {
				if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
					if (!Arrays.equals(kv.getQualifier(), kv_university.getQualifier())) {
						return;
					}
				}
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
				builder.append("\t" + triple[1] + "\t" + triple[2] + "\n");
			}
			context.write(key, new Text(builder.toString()));
		}
	}
}
