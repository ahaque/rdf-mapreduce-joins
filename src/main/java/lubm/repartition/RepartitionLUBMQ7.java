package lubm.repartition;

/**
 * Repartition Join LUBM Q7
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import lubm.sortmerge.LUBMSharedServices;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bsbm.repartition.CompositeGroupingComparator;
import bsbm.repartition.CompositeKeyWritable;
import bsbm.repartition.CompositePartitioner;
import bsbm.repartition.CompositeSortComparator;
import bsbm.sortmerge.KeyValueArrayWritable;
import bsbm.sortmerge.SharedServices;


public class RepartitionLUBMQ7 {
	
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
		job.setJobName("LUBM-Q7-Repartition");
		job.setJarByClass(RepartitionLUBMQ7.class);
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
		job.setNumReduceTasks(1);
		
		// Repartition settings
		job.setPartitionerClass(CompositePartitioner.class);
		job.setSortComparatorClass(CompositeSortComparator.class);
		job.setGroupingComparatorClass(CompositeGroupingComparator.class);
		
	
		FileOutputFormat.setOutputPath(job, new Path("output/LUBM-Q7-Repartition"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class RepartitionMapper extends TableMapper<CompositeKeyWritable, KeyValueArrayWritable> {
		
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
			// If this is TP-04
			if (Arrays.equals(value.getRow(), Professor.getBytes())) {
				List<KeyValue> toTransmit = new ArrayList<KeyValue>();
				// Get all courses this professor teaches
				for (KeyValue kv : value.list()) {
					if (Arrays.equals(kv.getValue(), "ub_teacherOf".getBytes())) {
						toTransmit.add(kv);
					}
				}
				// Send it to all the course reducers
				for (KeyValue kv : toTransmit) {
					KeyValue[] singleKv = new KeyValue[1];
					singleKv[0] = kv;
					context.write(new CompositeKeyWritable(kv.getQualifier(),1), new KeyValueArrayWritable(singleKv));
				}
				return;
			}
			
			List<KeyValue> courses = new ArrayList<KeyValue>();
			for (KeyValue kv : value.list()) {
				// TP-01
				if (LUBMSharedServices.isStudent(kv)) {
					if (!Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
						return;
					}
				} else if (Arrays.equals(kv.getValue(), "ub_takesCourse".getBytes())) {
					// TP-03
					courses.add(kv);
				}
			}
			if (courses.size() == 0) { return; }
			// Use the course as the key because the reducer will operate on the course, not student
			for (KeyValue kv : courses) {
				KeyValue[] singleKv = new KeyValue[1];
				singleKv[0] = kv;
				context.write(new CompositeKeyWritable(kv.getQualifier(),2), new KeyValueArrayWritable(singleKv));
			}
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class RepartitionReducer extends Reducer<CompositeKeyWritable, KeyValueArrayWritable, Text, Text> {

		public void reduce(CompositeKeyWritable key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
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
			
			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (Arrays.equals(kv.getValue(), "ub_takesCourse".getBytes())) {
						students.add(new String(kv.getRow()));
						course = new String(kv.getQualifier());
					} else if (Arrays.equals(kv.getValue(), "ub_teacherOf".getBytes())) {
						validTakesCourse = true;
					}
				}
			}
			if (validTakesCourse == true && students.size() > 0 && course != null) {
				for (String s : students) {
					context.write(new Text(s + "\t" + course), new Text());
				}
			}
		}
	}
}
