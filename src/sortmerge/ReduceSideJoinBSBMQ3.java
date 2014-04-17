package sortmerge;

/**
 * Reduce Side Join BSBM Q3
 * @date March 2014
 * @author Albert Haque
 */

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReduceSideJoinBSBMQ3 {
	
	// Begin Query Information
	private static String ProductType = "bsbm-inst_ProductType230";
	private static String ProductFeature1 = "bsbm-inst_ProductFeature39";
	private static String ProductFeature2 = "bsbm-inst_ProductFeature41";
	private static int x = 0;
	private static int y = 5000;
	private static String[] ProjectedVariables = {"rdfs_label"};
	// End Query Information
	
	private static byte[] CF_AS_BYTES = "o".getBytes();
	
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
		job.setJobName("BSBM-Q3-ReduceSideJoin");
		job.setJarByClass(ReduceSideJoinBSBMQ3.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
		scan.setMaxVersions(200);
		scan.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],        // input HBase table name
				scan,             // Scan instance to control CF and attribute selection
				ReduceSideJoin_Mapper.class,   // mapper
				Text.class,         // mapper output key
				KeyValueArrayWritable.class,  // mapper output value
				job);

		// Reducer settings
		job.setReducerClass(SharedServices.ReduceSideJoin_Reducer.class);    // reducer class
		job.setNumReduceTasks(1);    // at least one, adjust as required
	
		FileOutputFormat.setOutputPath(job, new Path("output/BSBMQ3"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class ReduceSideJoin_Mapper extends TableMapper<Text, KeyValueArrayWritable> {
		
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* BERLIN SPARQL BENHCMARK QUERY 3
		   ----------------------------------------
SELECT ?product ?label
WHERE {
	[TP-01] ?product rdfs:label ?label .
	[TP-02] ?product a %ProductType% .
	[TP-03] ?product bsbm:productFeature %ProductFeature1% .
	[TP-04] ?product bsbm:productPropertyNumeric1 ?p1 .
	[TP-05] FILTER ( ?p1 > %x% ) 
	[TP-06] ?product bsbm:productPropertyNumeric3 ?p3 .
	[TP-07] FILTER (?p3 < %y% )
	[TP-08] OPTIONAL { 
	[TP-09] 	?product bsbm:productFeature %ProductFeature2% .
	[TP-10] 	?product rdfs:label ?testVar
	[TP-11] }
	[TP-12] FILTER (!bound(?testVar)) 
}
ORDER BY ?label
LIMIT 10
		   ---------------------------------------
		 */
			// TP-09
			byte[] item9 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductFeature2));
			if (item9 == null) {
				// If this subject doesn't have this value, don't penalize it since it's OPTIONAL
			} else {
				String item9_str = new String(item9);
				if (!item9_str.equals("bsbm-voc_productFeature")) { return; }
			}
			
			// TP-02
			byte[] item2 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductType));
			if (item2 == null) { return; }
			String item2_str = new String(item2);
			if (!item2_str.equals("rdf_type")) { return; }
			
			// TP-03
			byte[] item3 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductFeature1));
			if (item3 == null) { return; }
			String item3_str = new String(item3);
			if (!item3_str.equals("bsbm-voc_productFeature")) { return; }
	
			// TP-04 Since this is a literal, the predicate is the column name
			byte[] item4 = value.getValue(CF_AS_BYTES, Bytes.toBytes("bsbm-voc_productPropertyNumeric1"));
			if (item4 == null) { return; }
			int number4 = ByteBuffer.wrap(item4).getInt();
			if (number4 <= x) { return; }
			
			// TP-07
			byte[] item7 = value.getValue(CF_AS_BYTES, Bytes.toBytes("bsbm-voc_productPropertyNumeric3"));
			if (item7 == null) { return; }
			int number7 = ByteBuffer.wrap(item7).getInt();
			if (number7 >= y) { return; }
			
			// Subject (Mapper Output: Key)
			text.set(new String(value.getRow()));
			
			// HBase row for that subject (Mapper Output: Value)
			List<KeyValue> entireRowAsList = value.list();
			KeyValue[] entireRow = new KeyValue[ProjectedVariables.length];
			
			int index = 0;
			for (int i = 0; i < entireRowAsList.size(); i++) {
				// Reduce data sent across network by writing only columns that we know will be used
				for (String project : ProjectedVariables) {
					if (new String(entireRowAsList.get(i).getQualifier()).equals(project)) {
						entireRow[index] = entireRowAsList.get(i);
						index++;
					}
				}
			}
	    	context.write(text, new KeyValueArrayWritable(entireRow));
		}
	}
		    
}
