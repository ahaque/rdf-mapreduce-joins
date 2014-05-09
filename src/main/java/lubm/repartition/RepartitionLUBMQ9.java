package lubm.repartition;

/**
 * Repartition Join LUBM Q9
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import lubm.sortmerge.LUBMSharedServices;

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

import bsbm.repartition.CompositeKeyWritable;
import bsbm.sortmerge.KeyValueArrayWritable;


public class RepartitionLUBMQ9 {
	
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
		job1.setJobName("LUBM-Q9-Repartition");
		job1.setJarByClass(RepartitionLUBMQ9.class);
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
		job1.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job1, new Path("output/LUBMQ9"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}
	
	
	public static class Stage1_RepartitionMapper extends TableMapper<CompositeKeyWritable, KeyValueArrayWritable> {
		
		private static String[] typesToCheck = {
				"ub_GraduateStudent",
				"ub_UndergraduateStudent",
				"ub_AssistantProfessor",
				"ub_AssociateProfessor",
				"ub_FullProfessor",
				"ub_Lecturer",
				"ub_Course",
				"ub_GraduateCourse"
		};
		
		private static String realRowTypeToQueryType(String realRowType) {
			String rowType = null;
			switch (realRowType) {
			case "ub_GraduateStudent": rowType = "ub_Student"; break;
			case "ub_UndergraduateStudent": rowType = "ub_Student"; break;
			case "ub_AssistantProfessor": rowType = "ub_Faculty"; break;
			case "ub_AssociateProfessor": rowType = "ub_Faculty"; break;
			case "ub_FullProfessor": rowType = "ub_Faculty"; break;
			case "ub_Lecturer": rowType = "ub_Faculty"; break;
			case "ub_Course": rowType = "ub_Course"; break;
			case "ub_GraduateCourse": rowType = "ub_Course"; break;
			}
			return rowType;
		}
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* LUBM QUERY 9
		   ----------------------------------------
			SELECT ?X, ?Y, ?Z
			WHERE
			{ [TP-01] ?X rdf:type ub:Student .
			  [TP-02] ?Y rdf:type ub:Faculty .
			  [TP-03] ?Z rdf:type ub:Course .
			  [TP-04] ?X ub:advisor ?Y .
			  [TP-05] ?Y ub:teacherOf ?Z .
			  [TP-06] ?X ub:takesCourse ?Z }
		   ---------------------------------------
		 */
			// Determine if this row is a student, faculty, or course
			String realRowType = LUBMSharedServices.getTypeFromHBaseRow(value, typesToCheck);
			if (realRowType == null) { return; }
			
			String rowType = realRowTypeToQueryType(realRowType);
			
			List<KeyValue> entireRowAsList = value.list();
			
			// If this row is a student
			if (rowType.equals("ub_Student")) {
				// Emit TP-04 and TP-06
				List<KeyValue> toTransmitCourseZ = new ArrayList<KeyValue>();
				KeyValue advisorKv = null;
				for (KeyValue kv : entireRowAsList) {
					if (Arrays.equals(kv.getValue(), "ub_advisor".getBytes())) {
						advisorKv = kv;
					} else if (Arrays.equals(kv.getValue(), "ub_takesCourse".getBytes())) {
						toTransmitCourseZ.add(kv);
					}
				}
				// If this student doesn't have an advisor or takes zero courses...
				if (advisorKv == null || toTransmitCourseZ.size() == 0) { return; }

				/* 
				 * Since a student can be enrolled in more than 1 course, send the correct KV to the correct course reducer
				 * Also send the student's advisor because we'll join
				 * ADVISOR and COURSE (to make sure ADVISOR teaches COURSE) on the reducer
				 */
				for (KeyValue kv : toTransmitCourseZ) {
					KeyValue[] smallArray = new KeyValue[2];
					smallArray[0] = kv;
					smallArray[1] = advisorKv;
					context.write(new CompositeKeyWritable(kv.getQualifier(),1), new KeyValueArrayWritable(smallArray));
				}
			}
			
			// If this row is a faculty
			else if (rowType.equals("ub_Faculty")) {
				ArrayList<KeyValue> teachesCourses = new ArrayList<KeyValue>();
				for (KeyValue kv : entireRowAsList) {
					// Emit TP-05
					if (Arrays.equals(kv.getValue(), "ub_teacherOf".getBytes())) {
						teachesCourses.add(kv);
					}
				}
				// Send the faculty to the course they teach
				for (KeyValue kv : teachesCourses) {
					KeyValue[] singleCourse = new KeyValue[1];
					singleCourse[0] = kv;
					context.write(new CompositeKeyWritable(kv.getQualifier(),2), new KeyValueArrayWritable(singleCourse));
				}
			}
			
			// If this row is a course
			// We technically don't use this KV in the reducer but we'll send it anyway in case
//			else if (rowType.equals("ub_Course")) {
//				for (KeyValue kv : entireRowAsList) {
//					if (Arrays.equals(kv.getValue(), "rdf_type".getBytes())) {
//						toTransmit.add(kv);
//					}
//				}
//				context.write(new CompositeKeyWritable(value.getRow(),3), new KeyValueArrayWritable(SharedServices.listToArray(toTransmit)));
//			}
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
			/* LUBM QUERY 9
			   ----------------------------------------
			SELECT ?X, ?Y, ?Z
			WHERE
			{ [TP-01] ?X rdf:type ub:Student .
			  [TP-02] ?Y rdf:type ub:Faculty .
			  [TP-03] ?Z rdf:type ub:Course .
			  [TP-04] ?X ub:advisor ?Y .
			  [TP-05] ?Y ub:teacherOf ?Z .
			  [TP-06] ?X ub:takesCourse ?Z }
			   ---------------------------------------
			 */
			
			// Key: Student, Value: Faculty (keys are students in this course)
			HashMap<String, String> studentHasAdvisor = new HashMap<String, String>();
			
			// List of faculty teaching this course
			HashSet<String> facultyTeachingThisCourse = new HashSet<String>();

			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (Arrays.equals(kv.getValue(), "ub_teacherOf".getBytes())) {
						facultyTeachingThisCourse.add(new String(kv.getRow()));
					} else if (Arrays.equals(kv.getValue(), "ub_advisor".getBytes())) {
						studentHasAdvisor.put(new String(kv.getRow()), new String(kv.getQualifier()));
					}
				}
			}
			
			for (String s : studentHasAdvisor.keySet()) {
				if (facultyTeachingThisCourse.contains(studentHasAdvisor.get(s))) {
					String triple = s + "\t" + studentHasAdvisor.get(s) + "\t" + key.getValue();
					context.write(new Text(triple), new Text());
				}
			}			
		}
	}
}
