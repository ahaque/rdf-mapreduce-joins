package lubm.sortmerge;

/**
 * SortMerge Join LUBM Q7
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import bsbm.sortmerge.KeyValueArrayWritable;


public class SortMergeLUBMQ7 {
	
	// Begin Query Information
	public static String Professor = "Department0.University0.edu/AssociateProfessor0";
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
		job.setJobName("LUBM-Q7-SortMerge");
		job.setJarByClass(SortMergeLUBMQ7.class);
		// Change caching to speed up the scan
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
		job.setReducerClass(SortMergeReducer.class);
	
		FileOutputFormat.setOutputPath(job, new Path("output/LUBM-Q7-SortMerge"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class SortMergeMapper extends TableMapper<Text, KeyValueArrayWritable> {
		
		private static Counter studentRowsIn;
		private static Counter courseRowsIn;
	    private static Counter mapperRowsIn;
	    private static Counter mapperRowsOut;
	    
	    private static Counter studentTriplesIn;
		private static Counter courseTriplesIn;
	    private static Counter mapperTriplesIn;
	    private static Counter mapperTriplesOut;

        protected void setup(Context context) throws IOException, InterruptedException {   
			courseRowsIn = context.getCounter(LUBM_ROW_COUNTERS.COURSES_IN);
			studentRowsIn = context.getCounter(LUBM_ROW_COUNTERS.STUDENTS_IN);
			mapperRowsIn = context.getCounter(LUBM_ROW_COUNTERS.MAPPER_IN);
			mapperRowsOut = context.getCounter(LUBM_ROW_COUNTERS.MAPPER_OUT);
			
			courseTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.COURSES_IN);
			studentTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.STUDENTS_IN);
			mapperTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.MAPPER_IN);
			mapperTriplesOut = context.getCounter(LUBM_TRIPLE_COUNTERS.MAPPER_OUT);
        }
		
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* LUBM QUERY 7
		   ----------------------------------------
			SELECT ?X, ?Y
			WHERE 
			{
			[TP-01] ?X rdf:type ub:Student .
			[TP-02] ?Y rdf:type ub:Course .
			[TP-03] ?X ub:takesCourse ?Y .
			[TP-04] <http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y
			}
		   ---------------------------------------
		 */
			mapperRowsIn.increment(1);
			// If this is TP-04 the professor
			if (Arrays.equals(value.getRow(), Professor.getBytes())) {
				List<KeyValue> toTransmit = new ArrayList<KeyValue>();
				// Get all courses this professor teaches
				for (KeyValue kv : value.list()) {
					mapperTriplesIn.increment(1);
					if (Arrays.equals(kv.getValue(), "ub_teacherOf".getBytes())) {
						toTransmit.add(kv);
					}
				}
				// Send it to all the course reducers
				for (KeyValue kv : toTransmit) {
					KeyValue[] singleKv = new KeyValue[1];
					singleKv[0] = kv;
					mapperTriplesOut.increment(1);
					mapperRowsOut.increment(1);
					context.write(new Text(kv.getQualifier()), new KeyValueArrayWritable(singleKv));
				}
				return;
			}
			
			boolean isThisRowAStudent = false;
			boolean isThisRowACourse = false;
			List<KeyValue> courses = new ArrayList<KeyValue>();
			List<KeyValue> rowAsList = value.list();
			for (KeyValue kv : rowAsList) {
				mapperTriplesIn.increment(1);
				// If this row is a course
				if (Arrays.equals(kv.getQualifier(), "ub_Course".getBytes())) {
					courseRowsIn.increment(1);
					isThisRowACourse = true;
				}
				// TP-01
				if (LUBMSharedServices.isStudent(kv)) {
					if (!Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
						return;
					}
					studentRowsIn.increment(1);
					isThisRowAStudent = true;
				} else if (Arrays.equals(kv.getValue(), "ub_takesCourse".getBytes())) {
					// TP-03
					courses.add(kv);
				}
			}
			if (isThisRowAStudent) {
				studentTriplesIn.increment(rowAsList.size());
			} else if (isThisRowACourse) {
				courseTriplesIn.increment(rowAsList.size());
			}
			
			if (courses.size() == 0) { return; }
			mapperRowsOut.increment(1);
			// Use the course as the key because the reducer will operate on the course, not student
			for (KeyValue kv : courses) {
				KeyValue[] singleKv = new KeyValue[1];
				singleKv[0] = kv;
				mapperTriplesOut.increment(1);
				context.write(new Text(kv.getQualifier()), new KeyValueArrayWritable(singleKv));
			}
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class SortMergeReducer extends Reducer<Text, KeyValueArrayWritable, Text, Text> {
		
		private static Counter studentRowsJoined;
		private static Counter courseRowsJoined;
	    private static Counter reducerRowsIn;
	    private static Counter reducerRowsOut;
	    
        private static Counter studentTriplesJoined;
        private static Counter courseTriplesJoined;
        private static Counter reducerTriplesIn;
        private static Counter reducerTriplesOut;

        protected void setup(Context context) throws IOException, InterruptedException {   
        	courseRowsJoined = context.getCounter(LUBM_ROW_COUNTERS.COURSES_JOINED);
        	studentRowsJoined = context.getCounter(LUBM_ROW_COUNTERS.STUDENTS_JOINED);
        	reducerRowsIn = context.getCounter(LUBM_ROW_COUNTERS.REDUCER_IN);
			reducerRowsOut = context.getCounter(LUBM_ROW_COUNTERS.REDUCER_OUT);
			
			courseTriplesJoined = context.getCounter(LUBM_TRIPLE_COUNTERS.COURSES_JOINED);
            studentTriplesJoined = context.getCounter(LUBM_TRIPLE_COUNTERS.STUDENTS_JOINED);
            reducerTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.REDUCER_IN);
            reducerTriplesOut = context.getCounter(LUBM_TRIPLE_COUNTERS.REDUCER_OUT);
        }

		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
			/* LUBM QUERY 7
			   ----------------------------------------
			SELECT ?X, ?Y
			WHERE 
			{
			[TP-01] ?X rdf:type ub:Student .
			[TP-02] ?Y rdf:type ub:Course .
			[TP-03] ?X ub:takesCourse ?Y .
			[TP-04] <http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y
			}
			   ---------------------------------------
			 */
			ArrayList<String> students = new ArrayList<String>();
			String course = null;
			boolean validTakesCourse = false;
			
			long studentTriplesArrived = 0;
			long reducerTriplesArrived = 0;
			for (KeyValueArrayWritable array : values) {
				reducerRowsIn.increment(1); // Each KeyValueArrayWritable represents a row sent from a mapper
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					reducerTriplesArrived++;
					if (Arrays.equals(kv.getValue(), "ub_takesCourse".getBytes())) {
						students.add(new String(kv.getRow()));
						course = new String(kv.getQualifier());
						studentTriplesArrived++;
					} else if (Arrays.equals(kv.getValue(), "ub_teacherOf".getBytes())) {
						validTakesCourse = true;
					}
				}
			}
			reducerTriplesIn.increment(reducerTriplesArrived);
			
			if (validTakesCourse == true && students.size() > 0 && course != null) {
				courseRowsJoined.increment(1); // The reducer represents a course so we know a join took place here
				reducerRowsOut.increment(1);
				studentTriplesJoined.increment(studentTriplesArrived);
				courseTriplesJoined.increment(1);
				reducerTriplesOut.increment(1); // Increment for course
				for (String s : students) {
					studentRowsJoined.increment(1);
					reducerRowsOut.increment(1);
					reducerTriplesOut.increment(1); // Increment for student
					context.write(new Text(s + "\t" + course), new Text());
				}
			}
		}
	}
}
