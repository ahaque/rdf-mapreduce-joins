package lubm.repartition;

/**
 * Repartition Join LUBM Q12
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bsbm.sortmerge.KeyValueArrayWritable;
import bsbm.repartition.CompositeGroupingComparator;
import bsbm.repartition.CompositeKeyWritable;
import bsbm.repartition.CompositePartitioner;
import bsbm.repartition.CompositeSortComparator;


public class RepartitionLUBMQ12 {
	
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
		job1.setJobName("LUBM-Q12-Repartition");
		job1.setJarByClass(RepartitionLUBMQ12.class);
		// Change caching and number of time stamps to speed up the scan
		scan1.setCaching(500);        
		scan1.setMaxVersions(3);
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

		FileOutputFormat.setOutputPath(job1, new Path("output/LUBM-Q12-Repartition"));
		
		job1.setPartitionerClass(CompositePartitioner.class);
		job1.setSortComparatorClass(CompositeSortComparator.class);
		job1.setGroupingComparatorClass(CompositeGroupingComparator.class);

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}
	
	
	public static class Stage1_RepartitionMapper extends TableMapper<CompositeKeyWritable, KeyValueArrayWritable> {
		
		private static Counter facultyRowsIn;
		private static Counter departmentRowsIn;
	    private static Counter mapperRowsIn;
	    private static Counter mapperRowsOut;
	    
        private static Counter facultyTriplesIn;
        private static Counter departmentTriplesIn;
        private static Counter mapperTriplesIn;
        private static Counter mapperTriplesOut;

        protected void setup(Context context) throws IOException, InterruptedException {   
        	facultyRowsIn = context.getCounter(LUBM_ROW_COUNTERS.FACULTY_IN);
        	departmentRowsIn = context.getCounter(LUBM_ROW_COUNTERS.DEPARTMENTS_IN);
			mapperRowsIn = context.getCounter(LUBM_ROW_COUNTERS.MAPPER_IN);
			mapperRowsOut = context.getCounter(LUBM_ROW_COUNTERS.MAPPER_OUT);
			
			facultyTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.FACULTY_IN);
	        departmentTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.DEPARTMENTS_IN);
	        mapperTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.MAPPER_IN);
	        mapperTriplesOut = context.getCounter(LUBM_TRIPLE_COUNTERS.MAPPER_OUT);
        }
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* LUBM QUERY 12
		   ----------------------------------------
			SELECT ?X, ?Y
			WHERE
			{ [TP-01] ?X rdf:type ub:Chair .
			  [TP-02] ?Y rdf:type ub:Department .
			  [TP-03] ?X ub:worksFor ?Y .
			  [TP-04] ?Y ub:subOrganizationOf <http://www.University0.edu>}
		   ---------------------------------------
		 */
			mapperRowsIn.increment(1);
			// Determine if this row is a student, faculty, or course
			List<KeyValue> entireRowAsList = value.list();
			int numTriplesInRow = entireRowAsList.size();
			mapperTriplesIn.increment(numTriplesInRow);
			
			String rowType = null;
			// Store some information so we only have to loop through all KVs once
			KeyValue xWorksForKv = null;
			KeyValue ySubOrgOfKv = null;
			
			for (KeyValue kv : entireRowAsList) {
				// If a faculty member contains the headOf predicate, they are the chair of that department
				if (Arrays.equals(kv.getValue(), "ub_headOf".getBytes())) {
					rowType = "faculty";
				} // Store faculty information for later
				else if (Arrays.equals(kv.getValue(), "ub_worksFor".getBytes())) {
					xWorksForKv = kv;
					facultyRowsIn.increment(1);
					facultyTriplesIn.increment(numTriplesInRow);
				}
				// Check if a department
				else if (Arrays.equals(kv.getQualifier(), "ub_Department".getBytes())) {
					if (Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
						rowType = "department";
						departmentRowsIn.increment(1);
						departmentTriplesIn.increment(numTriplesInRow);
					}
				}
				// Store subOrganization info for later
				else if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
					if (Arrays.equals(kv.getQualifier(), University.getBytes())) {
						ySubOrgOfKv = kv;
					}
				}
			}
			if (rowType == null || (xWorksForKv == null && ySubOrgOfKv == null)) {
				return;
			}
			
			KeyValue[] smallArray = new KeyValue[1];
			if (rowType.equals("faculty")) {
				smallArray[0] = xWorksForKv;
				mapperRowsOut.increment(1);
				mapperTriplesOut.increment(1);
				context.write(new CompositeKeyWritable(xWorksForKv.getQualifier(),1), new KeyValueArrayWritable(smallArray));
			} else if (rowType.equals("department") && ySubOrgOfKv != null) {
				mapperRowsOut.increment(1);
				mapperTriplesOut.increment(1);
				smallArray[0] = ySubOrgOfKv;
				context.write(new CompositeKeyWritable(ySubOrgOfKv.getRow(),2), new KeyValueArrayWritable(smallArray));
			}
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class Stage1_RepartitionReducer extends Reducer<CompositeKeyWritable, KeyValueArrayWritable, Text, Text> {
		
		private static Counter facultyRowsJoined;
		private static Counter departmentRowsJoined;
	    private static Counter reducerRowsIn;
	    private static Counter reducerRowsOut;
	    
		private static Counter facultyTriplesJoined;
		private static Counter departmentTriplesJoined;
	    private static Counter reducerTriplesIn;
	    private static Counter reducerTriplesOut;

        protected void setup(Context context) throws IOException, InterruptedException {   
        	facultyRowsJoined = context.getCounter(LUBM_ROW_COUNTERS.FACULTY_JOINED);
        	departmentRowsJoined = context.getCounter(LUBM_ROW_COUNTERS.DEPARTMENTS_JOINED);
        	reducerRowsIn = context.getCounter(LUBM_ROW_COUNTERS.REDUCER_IN);
        	reducerRowsOut = context.getCounter(LUBM_ROW_COUNTERS.REDUCER_OUT);
			
            facultyTriplesJoined = context.getCounter(LUBM_TRIPLE_COUNTERS.FACULTY_JOINED);
            departmentTriplesJoined = context.getCounter(LUBM_TRIPLE_COUNTERS.DEPARTMENTS_JOINED);
            reducerTriplesIn = context.getCounter(LUBM_TRIPLE_COUNTERS.REDUCER_IN);
            reducerTriplesOut = context.getCounter(LUBM_TRIPLE_COUNTERS.REDUCER_OUT);
        }

		public void reduce(CompositeKeyWritable key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
			/* LUBM QUERY 12
			   ----------------------------------------
			SELECT ?X, ?Y
			WHERE
			{ [TP-01] ?X rdf:type ub:Chair .
			  [TP-02] ?Y rdf:type ub:Department .
			  [TP-03] ?X ub:worksFor ?Y .
			  [TP-04] ?Y ub:subOrganizationOf <http://www.University0.edu>}
			   ---------------------------------------
			 */
			
			String chair = null;
			String department = null;
			boolean validSuborgOf = false;
			
			for (KeyValueArrayWritable array : values) {
				reducerRowsIn.increment(1);
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					reducerTriplesIn.increment(1);
					if (Arrays.equals(kv.getValue(), "ub_worksFor".getBytes())) {
						chair = new String(kv.getRow());
						department = new String(kv.getQualifier());
					} else if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
						validSuborgOf = true;
					}
				}
			}
			
			if (validSuborgOf == true && chair != null && department != null) {
				reducerRowsOut.increment(2); // One for faculty, one for department
				reducerTriplesOut.increment(2);
				facultyRowsJoined.increment(1);
				departmentRowsJoined.increment(1);
				facultyTriplesJoined.increment(1);
				departmentTriplesJoined.increment(1);
				context.write(new Text(chair + "\t" + department), new Text());
			}
		}
	}
}
