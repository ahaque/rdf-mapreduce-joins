package lubm.repartition;

/**
 * Repartition Join LUBM Q8
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bsbm.repartition.CompositeGroupingComparator;
import bsbm.repartition.CompositeKeyWritable;
import bsbm.repartition.CompositePartitioner;
import bsbm.repartition.CompositeSortComparator;
import bsbm.sortmerge.KeyValueArrayWritable;
import bsbm.sortmerge.SharedServices;
import lubm.sortmerge.LUBMSharedServices;

public class RepartitionLUBMQ8 {
	
	// Begin Query Information
	public static String University = "University0.edu";
	// End Query Information
		
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
		
		@SuppressWarnings("deprecation")
		Job job = new Job(hConf);
		job.setJobName("LUBM-Q8-Repartition");
		job.setJarByClass(RepartitionLUBMQ8.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
		scan.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],        // input HBase table name
				scan,             // Scan instance to control CF and attribute selection
				RepartitionMapper.class,   // mapper
				CompositeKeyWritable.class,         // mapper output key
				KeyValueArrayWritable.class,  // mapper output value
				job);

		// Reducer settings
		job.setReducerClass(RepartitionReducer.class);    // reducer class
		//job.setNumReduceTasks(1);    // at least one, adjust as required
		
		// Repartition settings
		job.setPartitionerClass(CompositePartitioner.class);
		job.setSortComparatorClass(CompositeSortComparator.class);
		job.setGroupingComparatorClass(CompositeGroupingComparator.class);
	
		FileOutputFormat.setOutputPath(job, new Path("output/LUBM-Q8-Repartition"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class RepartitionMapper extends TableMapper<CompositeKeyWritable, KeyValueArrayWritable> {
		
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
					context.write(new CompositeKeyWritable(xMemberOfY.getQualifier(),1), new KeyValueArrayWritable(smallArray));
				} else if (rowType.equals("department") && ySubOrgOfKv != null) {
					KeyValue[] smallArray = new KeyValue[1];
					smallArray[0] = ySubOrgOfKv;
					context.write(new CompositeKeyWritable(ySubOrgOfKv.getRow(),2), new KeyValueArrayWritable(smallArray));
				}
			}
	}
	

	public static class RepartitionReducer extends Reducer<CompositeKeyWritable, KeyValueArrayWritable, Text, Text> {

		public void reduce(CompositeKeyWritable key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
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
