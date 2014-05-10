package lubm.repartition;

/**
 * Repartition Join LUBM Q8
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import lubm.sortmerge.LUBMSharedServices;
import lubm.sortmerge.LUBMSharedServices.LUBM_ROW_COUNTERS;
import lubm.sortmerge.LUBMSharedServices.LUBM_TRIPLE_COUNTERS;

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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bsbm.repartition.CompositeGroupingComparator;
import bsbm.repartition.CompositeKeyWritable;
import bsbm.repartition.CompositePartitioner;
import bsbm.repartition.CompositeSortComparator;
import bsbm.sortmerge.KeyValueArrayWritable;

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
		
		private static Counter studentRowsIn;
		private static Counter departmentRowsIn;
	    private static Counter mapperRowsIn;
	    private static Counter mapperRowsOut;
	    
	    private static Counter studentTriplesIn;
		private static Counter departmentTriplesIn;
	    private static Counter mapperTriplesIn;
	    private static Counter mapperTriplesOut;

        protected void setup(Context context) throws IOException, InterruptedException {   
			departmentRowsIn = context.getCounter(LUBM_ROW_COUNTERS.DEPARTMENTS_IN);
			studentRowsIn = context.getCounter(LUBM_ROW_COUNTERS.STUDENTS_IN);
			mapperRowsIn = context.getCounter(LUBM_ROW_COUNTERS.MAPPER_IN);
			mapperRowsOut = context.getCounter(LUBM_ROW_COUNTERS.MAPPER_OUT);
			
			departmentTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.DEPARTMENTS_IN);
			studentTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.STUDENTS_IN);
			mapperTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.MAPPER_IN);
			mapperTriplesOut = context.getCounter(LUBM_TRIPLE_COUNTERS.MAPPER_OUT);
        }
		
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
			mapperRowsIn.increment(1);
			// Determine if this row is a student, faculty, or course
			List<KeyValue> entireRowAsList = value.list();
			mapperTriplesIn.increment(entireRowAsList.size());
			
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
						studentRowsIn.increment(1);
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
						departmentRowsIn.increment(1);
					}
				}
				// Store subOrganization info for later
				else if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
					if (Arrays.equals(kv.getQualifier(), University.getBytes())) {
						ySubOrgOfKv = kv;
					}
				}
			}
			// We must do this null check first
			if (rowType == null) {
				return;
			}
			// Before we terminate due to a failed join, we need to update the input counters
			if (rowType.equals("department")) {
				departmentTriplesIn.increment(entireRowAsList.size());
			} else if (rowType.equals("student")) {
				studentTriplesIn.increment(entireRowAsList.size());
			}
			// Now we can return if this row is not a valid join row (e.g. not a dept or student)
			if (xMemberOfY == null && ySubOrgOfKv == null) {
				return;
			}
			
			// Send everything to a DEPARTMENT reducer
			if (rowType.equals("student") && studentEmail != null) {
				KeyValue[] smallArray = new KeyValue[1];
				smallArray[0] = studentEmail;
				mapperRowsOut.increment(1);
				mapperTriplesOut.increment(1);
				context.write(new CompositeKeyWritable(xMemberOfY.getQualifier(),1), new KeyValueArrayWritable(smallArray));
			} else if (rowType.equals("department") && ySubOrgOfKv != null) {
				KeyValue[] smallArray = new KeyValue[1];
				smallArray[0] = ySubOrgOfKv;
				mapperRowsOut.increment(1);
				mapperTriplesOut.increment(1);
				context.write(new CompositeKeyWritable(ySubOrgOfKv.getRow(),2), new KeyValueArrayWritable(smallArray));
			}
		}
	}
	

	public static class RepartitionReducer extends Reducer<CompositeKeyWritable, KeyValueArrayWritable, Text, Text> {
		
        private static Counter studentRowsJoined;
        private static Counter departmentRowsJoined;
        private static Counter reducerRowsIn;
        private static Counter reducerRowsOut;
        
        private static Counter studentTriplesJoined;
        private static Counter departmentTriplesJoined;
        private static Counter reducerTriplesIn;
        private static Counter reducerTriplesOut;

        protected void setup(Context context) throws IOException, InterruptedException {   
            departmentRowsJoined = context.getCounter(LUBM_ROW_COUNTERS.DEPARTMENTS_JOINED);
            studentRowsJoined = context.getCounter(LUBM_ROW_COUNTERS.STUDENTS_JOINED);
            reducerRowsIn = context.getCounter(LUBM_ROW_COUNTERS.REDUCER_IN);
            reducerRowsOut = context.getCounter(LUBM_ROW_COUNTERS.REDUCER_OUT);
            
            departmentTriplesJoined = context.getCounter(LUBM_TRIPLE_COUNTERS.DEPARTMENTS_JOINED);
            studentTriplesJoined = context.getCounter(LUBM_TRIPLE_COUNTERS.STUDENTS_JOINED);
            reducerTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.REDUCER_IN);
            reducerTriplesOut = context.getCounter(LUBM_TRIPLE_COUNTERS.REDUCER_OUT);
        }

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
					reducerRowsIn.increment(1); // Each KeyValueArrayWritable represents a row sent from a mapper
					for (KeyValue kv : (KeyValue[]) array.toArray()) {
						reducerTriplesIn.increment(1);
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
					reducerRowsOut.increment(1); // for department row
					reducerTriplesOut.increment(1);
					departmentRowsJoined.increment(1);
					departmentTriplesJoined.increment(1); // (department, subOrgOf, university) triple
					for (String student : studentEmails.keySet()) {
						reducerRowsOut.increment(1); // for each student row
						studentRowsJoined.increment(1);
						studentTriplesJoined.increment(1); // (student,hasEmail,email) triple
						reducerTriplesOut.increment(1);
						context.write(new Text(student + "\t" + key.toString() + "\t" + studentEmails.get(student)), new Text());
					}
				}
			}
	}
}
