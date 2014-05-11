package lubm.repartition;

/**
 * Repartition Join LUBM Q2
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bsbm.repartition.CompositeGroupingComparator;
import bsbm.repartition.CompositeKeyWritable;
import bsbm.repartition.CompositePartitioner;
import bsbm.repartition.CompositeSortComparator;
import bsbm.sortmerge.KeyValueArrayWritable;


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
		
		FileOutputFormat.setOutputPath(job1, new Path("output/LUBM-Q2-Repartition"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}
	
	public static class Stage1_RepartitionMapper extends TableMapper<CompositeKeyWritable, KeyValueArrayWritable> {
		
		private static String[] typesToCheck = {
			"ub_GraduateStudent",
			"ub_University",
			"ub_Department",
		};
		
		private static Counter gradStudentRowsIn;
		private static Counter universityRowsIn;
		private static Counter departmentRowsIn;
	    private static Counter mapperRowsIn;
	    private static Counter mapperRowsOut;
	    
        private static Counter gradStudentTriplesIn;
        private static Counter universityTriplesIn;
        private static Counter departmentTriplesIn;
        private static Counter mapperTriplesIn;
        private static Counter mapperTriplesOut;
	    

        protected void setup(Context context) throws IOException, InterruptedException {   
			gradStudentRowsIn = context.getCounter(LUBM_ROW_COUNTERS.GRAD_STUDENTS_IN);
			universityRowsIn = context.getCounter(LUBM_ROW_COUNTERS.UNIVERSITIES_IN);
			departmentRowsIn = context.getCounter(LUBM_ROW_COUNTERS.DEPARTMENTS_IN);
			mapperRowsIn = context.getCounter(LUBM_ROW_COUNTERS.MAPPER_IN);
			mapperRowsOut = context.getCounter(LUBM_ROW_COUNTERS.MAPPER_OUT);
			
	        gradStudentTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.GRAD_STUDENTS_IN);
	        universityTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.UNIVERSITIES_IN);
	        departmentTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.DEPARTMENTS_IN);
	        mapperTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.MAPPER_IN);
	        mapperTriplesOut = context.getCounter(LUBM_TRIPLE_COUNTERS.MAPPER_OUT);
        }
		
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
			mapperRowsIn.increment(1);
			List<KeyValue> entireRowAsList = value.list();
			int numTriplesInRow =entireRowAsList.size();
			mapperTriplesIn.increment(numTriplesInRow);
			
			// Determine if this row is a grad student, university, or department
			// TP-01, TP-02, TP-03
			String realRowType = LUBMSharedServices.getTypeFromHBaseRow(value, typesToCheck);
			if (realRowType == null) {
				return;
			}

			// If this row is a grad student
			if (realRowType.equals("ub_GraduateStudent")) {
				gradStudentRowsIn.increment(1);
				gradStudentTriplesIn.increment(numTriplesInRow);
				// Emit TP-06
				KeyValue xUndergradFrom = null;
				KeyValue xMemberOfDept = null;
				for (KeyValue kv : entireRowAsList) {
					if (Arrays.equals(kv.getValue(), "ub_undergraduateDegreeFrom".getBytes())) {
						xUndergradFrom = kv;
					} else if (Arrays.equals(kv.getValue(), "ub_memberOf".getBytes())) {
						xMemberOfDept = kv;
					}
				}
				KeyValue[] toSend = new KeyValue[] {xMemberOfDept, xUndergradFrom};
				// Send the student to the reducer which handles their university
				// but also send their current department affiliation as well; we'll use this later
				mapperRowsOut.increment(1);
				mapperTriplesOut.increment(2);
				context.write(new CompositeKeyWritable(xUndergradFrom.getQualifier(),1), new KeyValueArrayWritable(toSend));
			}
			
			// If this row is a university
			else if (realRowType.equals("ub_University")) {
				// We already did the rdf_type check and since each reducer represents a university
				// We don't need to send anything
				universityRowsIn.increment(1);
				universityTriplesIn.increment(numTriplesInRow);
				mapperRowsOut.increment(1);
				mapperTriplesOut.increment(1);
			}
			
			// If this row is a department
			else if (realRowType.equals("ub_Department")) {
				departmentRowsIn.increment(1);
				departmentTriplesIn.increment(numTriplesInRow);
				KeyValue[] toSend = new KeyValue[1];
				for (KeyValue kv : entireRowAsList) {
					if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
						toSend[0] = kv;
					}
				}
				mapperRowsOut.increment(1);
				mapperTriplesOut.increment(1);
				context.write(new CompositeKeyWritable(toSend[0].getQualifier(),3), new KeyValueArrayWritable(toSend));
			}
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class Stage1_RepartitionReducer extends Reducer<CompositeKeyWritable, KeyValueArrayWritable, Text, Text> {

		
		private static Counter gradStudentRowsJoined;
		private static Counter departmentRowsJoined;
		private static Counter universityRowsJoined;
	    private static Counter reducerRowsIn;
	    private static Counter reducerRowsOut;
	    
		private static Counter gradStudentTriplesJoined;
		private static Counter departmentTriplesJoined;
		private static Counter universityTriplesJoined;
;
        private static Counter reducerTriplesIn;
        private static Counter reducerTriplesOut;

        protected void setup(Context context) throws IOException, InterruptedException {   
        	gradStudentRowsJoined = context.getCounter(LUBM_ROW_COUNTERS.GRAD_STUDENTS_JOINED);
        	departmentRowsJoined = context.getCounter(LUBM_ROW_COUNTERS.DEPARTMENTS_JOINED);
        	universityRowsJoined = context.getCounter(LUBM_ROW_COUNTERS.UNIVERSITIES_JOINED);
        	reducerRowsIn = context.getCounter(LUBM_ROW_COUNTERS.REDUCER_IN);
			reducerRowsOut = context.getCounter(LUBM_ROW_COUNTERS.REDUCER_OUT);
			
            gradStudentTriplesJoined = context.getCounter(LUBM_TRIPLE_COUNTERS.GRAD_STUDENTS_JOINED);
            departmentTriplesJoined = context.getCounter(LUBM_TRIPLE_COUNTERS.DEPARTMENTS_JOINED);
            universityTriplesJoined = context.getCounter(LUBM_TRIPLE_COUNTERS.UNIVERSITIES_JOINED);
            reducerTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.REDUCER_IN);
            reducerTriplesOut = context.getCounter(LUBM_TRIPLE_COUNTERS.REDUCER_OUT);
        }
		
        /**
         * REDUCER KEY: UNIVERSITY
         * REDUCER VALUES:	(student ub_memberOf department)
         * 					(student undergradFrom university)
         * 					(department suborgOf university) */
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
			// The students X at this reducer went to Y university for their undergrad
			// We have to check if they are part of a department Z where dept Z is part of university Y
			HashMap<String, String> studentMemberOfDepartment = new HashMap<String, String>();
			HashSet<String> departmentsPartOfThisUniversity = new HashSet<String>();

			// Populate the hash data structures
			for (KeyValueArrayWritable array : values) {
				reducerRowsIn.increment(1);
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					reducerTriplesIn.increment(1);
					if (Arrays.equals(kv.getValue(), "ub_memberOf".getBytes())) {
						studentMemberOfDepartment.put(new String(kv.getRow()), new String(kv.getQualifier()));
					} else if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
						departmentsPartOfThisUniversity.add(new String(kv.getRow()));
					}
				}
			}
			
			// We know TP-06 has been satisfied. Now we need to check TP-05. (TP-04 was handled by the map phase)
			for (String s : studentMemberOfDepartment.keySet()) {
				// If the student's current department is a part of this (reducer's key) university
				if (departmentsPartOfThisUniversity.contains(studentMemberOfDepartment.get(s))) {
					// (student memberOf department), (dept subOrg of University), (student undergradFrom university)
					reducerTriplesOut.increment(3); 
					reducerRowsOut.increment(3);
					gradStudentRowsJoined.increment(1);
					departmentRowsJoined.increment(1);
					universityRowsJoined.increment(1);
					gradStudentTriplesJoined.increment(2);
					departmentTriplesJoined.increment(1);
					universityTriplesJoined.increment(1);
					context.write(new Text(s + "\t" + studentMemberOfDepartment.get(s) + "\t" + key), new Text());
				}
			}
		}
	}
}
