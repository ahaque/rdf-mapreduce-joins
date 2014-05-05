package lubm.sortmerge;

/**
 * Sort Merge Join LUBM Q2
 * @date May 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bsbm.sortmerge.KeyValueArrayWritable;
import bsbm.sortmerge.SharedServices;


public class SortMergeLUBMQ2 {
	
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
		
		Job job = new Job(hConf);
		job.setJobName("LUBM-Q2-SortMerge");
		job.setJarByClass(SortMergeLUBMQ2.class);
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
		job.setReducerClass(SortMergeReducer.class);    // reducer class
		//job.setNumReduceTasks(1);    // at least one, adjust as required
	
		FileOutputFormat.setOutputPath(job, new Path("output/LUBMQ2"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class SortMergeMapper extends TableMapper<Text, KeyValueArrayWritable> {
		
		private Text text = new Text();

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
			// TriplePattern-01
			byte[] item1 = value.getValue(SharedServices.CF_AS_BYTES, Bytes.toBytes("ub_GraduateStudent"));
			if (item1 == null) { return; }
			String item1_str = new String(item1);
			if (!item1_str.equals("rdf_type")) { return; }
			
			// Subject (Mapper Output: Key)
			text.set(new String(value.getRow()));
			
			// HBase row for that subject (Mapper Output: Value)
			List<KeyValue> entireRowAsList = value.list();
			List<KeyValue> toTransmitList = new ArrayList<KeyValue>();
			
			// Get the KVs to transmit
			for (int i = 0; i < entireRowAsList.size(); i++) {
				// Reduce data sent across network by writing only columns that we know will be used
				if (Arrays.equals(entireRowAsList.get(i).getValue(), "ub_memberOf".getBytes())) {
					toTransmitList.add(entireRowAsList.get(i));
				} else if (Arrays.equals(entireRowAsList.get(i).getValue(), "ub_undergraduateDegreeFrom".getBytes())) {
					toTransmitList.add(entireRowAsList.get(i));
				}
			}
	    	context.write(text, new KeyValueArrayWritable(SharedServices.listToArray(toTransmitList)));
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class SortMergeReducer extends Reducer<Text, KeyValueArrayWritable, Text, Text> {

		HTable table;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			table = new HTable(conf, conf.get("scan.table"));
		}

		public void reduce(Text key, Iterable<KeyValueArrayWritable> values,
				Context context) throws IOException, InterruptedException {
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

			List<KeyValue> finalKeyValues = new ArrayList<KeyValue>();

			// Find and get university information
			// TP-06
			KeyValue kv_university = null;
			KeyValue kv_department = null;
			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (Arrays.equals(kv.getValue(), "ub_undergraduateDegreeFrom".getBytes())) {
						kv_university = kv;
						finalKeyValues.add(kv);
					} else if (Arrays.equals(kv.getValue(), "ub_memberOf".getBytes())) {
						kv_department = kv;
						finalKeyValues.add(kv);
					}
				}
			}
			if (kv_university == null) {
				return;
			}
			// TP-02
			Get g = new Get(kv_university.getQualifier());
			g.addColumn(SharedServices.CF_AS_BYTES, "ub_University".getBytes());
			Result universityResult = table.get(g);
			byte[] universityPredicate = universityResult.getValue(SharedServices.CF_AS_BYTES,"ub_University".getBytes());
			if (!Arrays.equals(universityPredicate, "rdf_type".getBytes())) {
				return;
			}
			
			// Find and get department information
			// TP-04
			if (kv_department == null) {
				return;
			}
			// TP-03
			Result departmentResult = table.get(new Get(kv_department.getQualifier()));
			// TP-05
			for (KeyValue kv : departmentResult.list()) {
				if (Arrays.equals(kv.getValue(), "ub_subOrganizationOf".getBytes())) {
					if (!Arrays.equals(kv.getQualifier(), kv_university.getQualifier())) {
						return;
					}
				}
			}

			// Format and output the values
			StringBuilder builder = new StringBuilder();
			builder.append("\n");
			for (KeyValue kv : finalKeyValues) {
				String[] triple = null;
				try {
					triple = SharedServices.keyValueToTripleString(kv);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				builder.append("\t" + triple[1] + "\t" + triple[2] + "\n");
			}
			context.write(key, new Text(builder.toString()));
		}
	}
}
