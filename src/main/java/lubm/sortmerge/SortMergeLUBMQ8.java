package lubm.sortmerge;

/**
 * Sort Merge Join LUBM Q8
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bsbm.sortmerge.KeyValueArrayWritable;


public class SortMergeLUBMQ8 {
	
	// Begin Query Information
	private static String University = "University0.edu";
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
	
	public static Job startJob_Stage1(String[] args, Configuration hConf) throws IOException {
		
	    Scan scan1 = new Scan();		
		@SuppressWarnings("deprecation")
		Job job1 = new Job(hConf);
		job1.setJobName("LUBM-Q8-SortMerge");
		job1.setJarByClass(SortMergeLUBMQ8.class);
		// Change caching and number of time stamps to speed up the scan
		scan1.setCaching(500);        
		scan1.setMaxVersions(3);
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
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job1, new Path("output/LUBM-Q8-SortMerge"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}
	
	
	public static class Stage1_SortMergeMapper extends TableMapper<Text, KeyValueArrayWritable> {
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* LUBM QUERY 8
		   ----------------------------------------
			SELECT ?X, ?Y, ?Z
			WHERE
			{ [TP-01] ?X rdf:type ub:Student .
			  [TP-02] ?Y rdf:type ub:Department .
			  [TP-03] ?X ub:memberOf ?Y .
			  [TP-04] ?Y ub:subOrganizationOf <http://www.University0.edu> .
			  [TP-05] ?X ub:emailAddress ?Z }
		   ---------------------------------------
		 */
			// Determine if this row is a student, faculty, or course
			List<KeyValue> entireRowAsList = value.list();
			
			String rowType = null;
			// Store some information so we only have to loop through all KVs once
			KeyValue xMemberOfY = null;
			KeyValue ySubOrgOfKv = null;
			KeyValue studentEmail = null;
			
			for (KeyValue kv : entireRowAsList) {
				// Is this a student?
				if (LUBMSharedServices.isStudent(kv)) {
					if (Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
						rowType = "student";
					}
				} // Store student information for later
				else if (Arrays.equals(kv.getValue(), "ub_memberOf".getBytes())) {
					xMemberOfY = kv;
				}
				// Get Email address of student
				// NOTE: This is a literal type (i.e. predicate is the qualifier)
				else if (Arrays.equals(kv.getQualifier(), "ub_emailAddress".getBytes())) {
					studentEmail = kv;
				}
				// Check if a department
				else if (Arrays.equals(kv.getQualifier(), "ub_Department".getBytes())) {
					if (Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
						rowType = "department";
					}
				}
				// Store subOrganization info for later
				else if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
					if (!Arrays.equals(kv.getQualifier(), University.getBytes())) {
						return;
					}
					ySubOrgOfKv = kv;
				}
			}
			if (rowType == null || (xMemberOfY == null && ySubOrgOfKv == null)) {
				return;
			}
			
			// Send everything to a DEPARTMENT reducer
			if (rowType.equals("student") && studentEmail != null) {
				KeyValue[] smallArray = new KeyValue[2];
				smallArray[0] = xMemberOfY;
				smallArray[1] = studentEmail;
				context.write(new Text(xMemberOfY.getQualifier()), new KeyValueArrayWritable(smallArray));
			} else if (rowType.equals("department") && ySubOrgOfKv != null) {
				KeyValue[] smallArray = new KeyValue[1];
				smallArray[0] = ySubOrgOfKv;
				context.write(new Text(ySubOrgOfKv.getRow()), new KeyValueArrayWritable(smallArray));
			}
		}
	}
	
	public static class Stage1_SortMergeReducer extends Reducer<Text, KeyValueArrayWritable, Text, Text> {

		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
		/* LUBM QUERY 8
		   ----------------------------------------
			SELECT ?X, ?Y, ?Z
			WHERE
			{ [TP-01] ?X rdf:type ub:Student .
			  [TP-02] ?Y rdf:type ub:Department .
			  [TP-03] ?X ub:memberOf ?Y .
			  [TP-04] ?Y ub:subOrganizationOf <http://www.University0.edu> .
			  [TP-05] ?X ub:emailAddress ?Z }
		   ---------------------------------------
		 */
			
			boolean validSuborgOf = false;
			HashMap<String,String> studentEmails = new HashMap<String, String>();
			
			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					// Populate the emails
					if (Arrays.equals(kv.getQualifier(), "ub_emailAddress".getBytes())) {
						studentEmails.put(new String(kv.getRow()), new String(kv.getValue()));
					}
					// Verify this department is a suborg of the University
					else if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
						validSuborgOf = true;
					}
				}
			}
			
			if (validSuborgOf == true && studentEmails.size() > 0) {
				for (String student : studentEmails.keySet()) {
					context.write(new Text(student + "\t" + key.toString() + "\t" + studentEmails.get(student)), new Text());
				}
			}
		}
	}
}
