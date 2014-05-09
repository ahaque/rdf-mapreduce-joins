package lubm.sortmerge;

/**
 * Sort Merge Join LUBM Q12
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bsbm.sortmerge.KeyValueArrayWritable;


public class SortMergeLUBMQ12 {
	
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
		job1.setJobName("LUBM-Q12-SortMerge");
		job1.setJarByClass(SortMergeLUBMQ12.class);
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
		job1.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job1, new Path("output/LUBM-Q12-SortMerge"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}
	
	
	public static class Stage1_SortMergeMapper extends TableMapper<Text, KeyValueArrayWritable> {
		
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
			// Determine if this row is a student, faculty, or course
			List<KeyValue> entireRowAsList = value.list();
			
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
			if (rowType == null || (xWorksForKv == null && ySubOrgOfKv == null)) {
				return;
			}
			
			KeyValue[] smallArray = new KeyValue[1];
			if (rowType.equals("faculty")) {
				smallArray[0] = xWorksForKv;
				context.write(new Text(xWorksForKv.getQualifier()), new KeyValueArrayWritable(smallArray));
			} else if (rowType.equals("department") && ySubOrgOfKv != null) {
				smallArray[0] = ySubOrgOfKv;
				context.write(new Text(ySubOrgOfKv.getRow()), new KeyValueArrayWritable(smallArray));
			}
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class Stage1_SortMergeReducer extends Reducer<Text, KeyValueArrayWritable, Text, Text> {

		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
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
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (Arrays.equals(kv.getValue(), "ub_worksFor".getBytes())) {
						chair = new String(kv.getRow());
						department = new String(kv.getQualifier());
					} else if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
						validSuborgOf = true;
					}
				}
			}
			
			if (validSuborgOf == true && chair != null && department != null) {
				context.write(new Text(chair + "\t" + department), new Text());
			}
		}
	}
}
