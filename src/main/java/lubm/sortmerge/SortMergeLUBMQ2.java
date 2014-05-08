package lubm.sortmerge;

/**
 * Sort Merge Join LUBM Q2
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

import bsbm.sortmerge.KeyValueArrayWritable;
import bsbm.sortmerge.SharedServices;


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
		@SuppressWarnings("deprecation")
		Job job1 = new Job(hConf);
		job1.setJobName("LUBM-Q2-SortMerge-Stage1");
		job1.setJarByClass(SortMergeLUBMQ2.class);
		// Change caching and number of time stamps to speed up the scan
		scan1.setCaching(500);        
		scan1.setMaxVersions(200);
		scan1.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],		// Input HBase table name
				scan1,			// Scan instance to control CF and attribute selection
				Stage1_SortMergeMapper.class,	// MAP: Class
				Text.class,		// MAP: Output Key
				KeyValueArrayWritable.class,  	// MAP: Output Value
				job1);

		// Reducer settings
		job1.setReducerClass(Stage1_SortMergeReducer.class);  
		//job1.setReducerClass(SharedServices.ReduceSideJoin_Reducer.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setNumReduceTasks(1);
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
			byte[] y_type = value.getValue(SharedServices.CF_AS_BYTES, Bytes.toBytes("ub_University"));
			byte[] z_type = value.getValue(SharedServices.CF_AS_BYTES, Bytes.toBytes("ub_Department"));

			List<KeyValue> entireRowAsList = value.list();
			List<KeyValue> toTransmit = new ArrayList<KeyValue>();
			
			// If this row is a grad student
			if (x_type != null) {
				// Emit TP-06
				byte[] y_reducerKey = null;
				byte[] z_reducerKey = null;
				List<KeyValue> toTransmitY = new ArrayList<KeyValue>();
				List<KeyValue> toTransmitZ = new ArrayList<KeyValue>();
				for (KeyValue kv : entireRowAsList) {
					if (Arrays.equals(kv.getValue(), "ub_undergraduateDegreeFrom".getBytes())) {
						toTransmitY.add(kv);
						// y_reducer key is the university
						y_reducerKey = kv.getQualifier();
					} else if (Arrays.equals(kv.getValue(), "ub_memberOf".getBytes())) {
						toTransmitZ.add(kv);
						// z_reducer key is the department
						z_reducerKey = kv.getQualifier();
					} else if (Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
						toTransmitY.add(kv);
						toTransmitZ.add(kv);
					} 
				}
				// Send this twice. Once for joining with Y and once for joining with Z
				context.write(new Text(y_reducerKey), new KeyValueArrayWritable(SharedServices.listToArray(toTransmitY)));
				context.write(new Text(z_reducerKey), new KeyValueArrayWritable(SharedServices.listToArray(toTransmitZ)));
			} 
			
			// If this row is a university
			else if (y_type != null) {
				for (KeyValue kv : entireRowAsList) {
					if (Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
						toTransmit.add(kv);
					}
				}
				context.write(new Text(value.getRow()), new KeyValueArrayWritable(SharedServices.listToArray(toTransmit)));
			}
			
			// If this row is a department
			else if (z_type != null) {
				for (KeyValue kv : entireRowAsList) {
					if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
						toTransmit.add(kv);
					} else if (Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
						toTransmit.add(kv);
					} 
				}
				context.write(new Text(value.getRow()), new KeyValueArrayWritable(SharedServices.listToArray(toTransmit)));
			} 
			
			// If this row is something else
			else {
				return;
			}
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class Stage1_SortMergeReducer extends Reducer<Text, KeyValueArrayWritable, Text, Text> {

		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
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

			// Find out if this is X JOIN Y or X JOIN Z
			for (KeyValueArrayWritable array : values) {
				String join = null;
				String gradStudent = null;
				String university = null;
				String department = null;
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					// Grad student getting sent to Z (university) reducer
					if (Arrays.equals(kv.getValue(), "ub_memberOf".getBytes())) {
						join = "XZ";
						gradStudent = new String(kv.getRow());
						department = key.toString();
						context.write(new Text(gradStudent + "\t ub_memberOf \t" + department), new Text(""));
						break;
					}
					// Department getting sent to Z (university) reducer
					else if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
						join = "XZ";
						department = key.toString();
						university = new String(kv.getQualifier());
						context.write(new Text(department + "\t ub_subOrganizationOf \t" + university), new Text(""));
						break;
					}
					// Grad student getting sent to Y reducer
					else if (Arrays.equals(kv.getValue(), "ub_undergraduateDegreeFrom".getBytes())) {
						join = "XY";
						gradStudent = new String(kv.getRow());
						university = key.toString();
						context.write(new Text(gradStudent + "\t ub_undergraduateDegreeFrom \t" + university), new Text(""));
						break;
					}
				}
				if (join == null) {
					return;
				}
				if (join.equals("XZ")) {
					continue;
				} else {
					continue;
				}
			}
		}
	}
}
