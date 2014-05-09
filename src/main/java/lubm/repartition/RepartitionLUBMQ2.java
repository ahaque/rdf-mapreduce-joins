package lubm.repartition;

/**
 * Repartition Join LUBM Q2
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bsbm.repartition.CompositeGroupingComparator;
import bsbm.repartition.CompositeKeyWritable;
import bsbm.repartition.CompositePartitioner;
import bsbm.repartition.CompositeSortComparator;
import bsbm.sortmerge.KeyValueArrayWritable;
import bsbm.sortmerge.SharedServices;


public class RepartitionLUBMQ2 {
	
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
		startJob_Stage2(args);
	}
	
	public static Job startJob_Stage1(String[] args, Configuration hConf) throws IOException {
		
	    Scan scan1 = new Scan();		
		@SuppressWarnings("deprecation")
		Job job1 = new Job(hConf);
		job1.setJobName("LUBM-Q2-Repartition-Stage1");
		job1.setJarByClass(RepartitionLUBMQ2.class);
		// Change caching and number of time stamps to speed up the scan
		scan1.setCaching(500);        
		scan1.setMaxVersions(200);
		scan1.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],		// Input HBase table name
				scan1,			// Scan instance to control CF and attribute selection
				Stage1_RepartitionMapper.class,	// MAP: Class
				CompositeKeyWritable.class,		// MAP: Output Key
				KeyValueArrayWritable.class,  	// MAP: Output Value
				job1);

		// Reducer settings
		job1.setReducerClass(Stage1_RepartitionReducer.class);  
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		// Repartition settings
		job1.setPartitionerClass(CompositePartitioner.class);
		job1.setSortComparatorClass(CompositeSortComparator.class);
		job1.setGroupingComparatorClass(CompositeGroupingComparator.class);
		
		FileOutputFormat.setOutputPath(job1, new Path("output/LUBM-Q2-Repartition/Stage1"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}
	
	public static Job startJob_Stage2(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        
	    @SuppressWarnings("deprecation")
		Job job = new Job(conf, "LUBM-Q2-Repartition-Stage2");
	    job.setJarByClass(RepartitionLUBMQ2.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(Stage2_RepartitionMapper.class);
	    job.setReducerClass(Stage2_RepartitionReducer.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setNumReduceTasks(1);
	        
	    FileInputFormat.addInputPath(job, new Path("output/LUBM-Q2-Repartition/Stage1"));
	    FileOutputFormat.setOutputPath(job, new Path("output/LUBM-Q2-Repartition/Stage2"));

	    try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	 }
	
	
	
	public static class Stage1_RepartitionMapper extends TableMapper<CompositeKeyWritable, KeyValueArrayWritable> {
		
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
				context.write(new CompositeKeyWritable(y_reducerKey,1), new KeyValueArrayWritable(SharedServices.listToArray(toTransmitY)));
				context.write(new CompositeKeyWritable(z_reducerKey,1), new KeyValueArrayWritable(SharedServices.listToArray(toTransmitZ)));
			}
			
			// If this row is a university
			else if (y_type != null) {
				for (KeyValue kv : entireRowAsList) {
					if (Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
						toTransmit.add(kv);
					}
				}
				context.write(new CompositeKeyWritable(value.getRow(),2), new KeyValueArrayWritable(SharedServices.listToArray(toTransmit)));
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
				context.write(new CompositeKeyWritable(value.getRow(),3), new KeyValueArrayWritable(SharedServices.listToArray(toTransmit)));
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
	public static class Stage1_RepartitionReducer extends Reducer<CompositeKeyWritable, KeyValueArrayWritable, Text, Text> {

		public void reduce(CompositeKeyWritable key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
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
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					// X JOIN Z
					if (Arrays.equals(kv.getValue(), "ub_memberOf".getBytes())) {
						String temp = new String(kv.getRow()) + "\t" + new String(kv.getValue()) + "\t" + new String(kv.getQualifier());
						context.write(new Text("XZ"), new Text(temp));
						break;
					}
					// X JOIN Y
					else if (Arrays.equals(kv.getValue(), "ub_undergraduateDegreeFrom".getBytes())) {
						String temp = new String(kv.getRow()) + "\t" + new String(kv.getValue()) + "\t" + new String(kv.getQualifier());
						context.write(new Text("XY"), new Text(temp));
						break;
					}
					// SETTING UP FOR Y JOIN Z - We output the Z tuples (departments)
					else if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
						String temp = new String(kv.getRow()) + "\t" + new String(kv.getValue()) + "\t" + new String(kv.getQualifier());
						context.write(new Text("ZY"), new Text(temp));
						break;
					}
				}
			}
		}
	}
	
	/**
	 * LUBM Q2 Stage 2 Mapper
	 * Read the intermediate results from Stage 1, send to reducers for Y JOIN Z
	 * @author Albert
	 */
	public static class Stage2_RepartitionMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s+");
			// line[0, 1, 2, 3]
			// <join> <subject> <predicate> <object>
			// We're sending everything to the university
			// LINE: <grad student> <undergradFrom> <university>
			if (line[0].equals("XY")) {
				context.write(new Text(line[3]), new Text(line[1] + "\t" + line[2] + "\t" + line[3]));
			} else
			// LINE: <grad student> <member of> <department>
			if (line[0].equals("XZ")) {
				String university = line[3].substring(line[3].indexOf("University"));
				context.write(new Text(university), new Text(line[1] + "\t" + line[2] + "\t" + line[3]));
			}
			// Send the department
			// LINE: <department> <subOrgOf> <university>
			else if (line[0].equals("ZY")) {
				context.write(new Text(line[3]), new Text(line[1] + "\t" + line[2] + "\t" + line[3]));
			}
		}
	}
	
	public static class Stage2_RepartitionReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Key: department, Value: grad students
			HashMap<String, ArrayList<String>> gradStudentDepartment = new HashMap<String, ArrayList<String>>();
			HashSet<String> gradStudentUndergradVerified = new HashSet<String>();
			HashSet<String> departmentVerified = new HashSet<String>();

			// Perform the Y JOIN Z operation
			for (Text val : values) {
				String[] line = val.toString().split("\\s+");
				// If this department is a suborg of the university, then join condition is true
				// We already know this is true otherwise this tuple won't be here
				if (line[1].equals("ub_subOrganizationOf")) {
					departmentVerified.add(line[0]);
				}
				// Make sure the grad student went here for undergrad
				else if (line[1].equals("ub_undergraduateDegreeFrom")) {
					gradStudentUndergradVerified.add(line[0]);
				}
				// We know the grad student is a member of this department and went to KEY for undergrad
				else if (line[1].equals("ub_memberOf")) {
					if (gradStudentDepartment.containsKey(line[2])) {
						gradStudentDepartment.get(line[2]).add(line[0]);
					} else {
						ArrayList<String> students = new ArrayList<String>();
						students.add(line[0]);
						gradStudentDepartment.put(line[2], students);
					}
				}
			}
			
			// Only write the departments that belong in this university
			for (String department : departmentVerified) {
				for (String student : gradStudentDepartment.get(department)) {
					if (!gradStudentUndergradVerified.contains(student)) {
						continue;
					}
					String result = student + "\t" + key.toString() + "\t" + department;
					context.write(new Text(result), new Text());
				}
			}
			
		}
	}
}
