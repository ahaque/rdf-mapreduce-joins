/**
 * Reduce Side Join Template for BSBM Benchmark
 * @date March 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceSideJoin {
	
	// Begin Query Information
	private static String ProductFeature1 = "bsbm-inst_ProductFeature8022";
	private static String ProductFeature2 = "bsbm-inst_ProductFeature52";
	private static String ProductType = "bsbm-inst_ProductType251";
	private static int x = 0;
	// End Query Information
	
	private static byte[] CF_AS_BYTES = "o".getBytes();
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		String USAGE_MSG = "  Arguments: <table name> <zk quorum>";

		if (args == null || args.length != 2) {
			System.out.println("\n" + USAGE_MSG);
			System.out.println("You entered " + args.length + " arguments.");
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
		job.setJobName("Reduce Side Join");
		job.setJarByClass(ReduceSideJoin.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
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
		job.setReducerClass(ReduceSideJoin_Reducer.class);    // reducer class
		job.setNumReduceTasks(1);    // at least one, adjust as required
	
		
		FileOutputFormat.setOutputPath(job, new Path("output/2014-03-21"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class ReduceSideJoin_Mapper extends TableMapper<Text, KeyValueArrayWritable> {
		
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			/* TP=Triple Pattern
			 * BSBM Query 1
			   SELECT DISTINCT ?product ?label
				WHERE { 
				 [TP-1] ?product rdfs:label ?label .  
				 [TP-2] ?product a %ProductType% .
				 [TP-3] ?product bsbm:productFeature %ProductFeature1% . 
				 [TP-4] ?product bsbm:productFeature %ProductFeature2% . 
				 [TP-5] ?product bsbm:productPropertyNumeric1 ?value1 . 
				 [TP-6] FILTER (?value1 > %x%) 
					}
				ORDER BY ?label
				LIMIT 10
			 */
			// TP-2
			byte[] item2 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductType));
			if (item2 == null) { return; }
			String item2_str = new String(item2);
			if (!item2_str.equals("rdf_type")) { return; }
			
			// TP-3
			byte[] item3 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductFeature1));
			if (item3 == null) { return; }
			String item3_str = new String(item3);
			if (!item3_str.equals("bsbm-voc_productFeature")) { return; }
	
			// TP-4
			byte[] item4 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductFeature2));
			if (item4 == null) { return; }
			String item4_str = new String(item4);
		    if (!item4_str.equals("bsbm-voc_productFeature")) { return; }

			// TP-6 - Since this is a literal, the predicate is the column name
			byte[] item6 = value.getValue(CF_AS_BYTES, Bytes.toBytes("bsbm-voc_productPropertyNumeric1"));
			if (item6 == null) { return; }
			int number6 = ByteBuffer.wrap(item6).getInt();
			if (number6 <= x) { return; }
			
			// Tuple is the project part of the SPARQL query
			text.set(new String(value.getRow()));
			List<KeyValue> entireRowAsList = value.list();
			KeyValue[] entireRow = new KeyValue[entireRowAsList.size()];
			
			for (int i = 0; i < entireRowAsList.size(); i++) {
				entireRow[i] = entireRowAsList.get(i);
			}
			
//			 Mapper Output Key: Row key/subject
//		     Mapper Output Value: all cells for the row/subject
	    	context.write(text, new KeyValueArrayWritable(entireRow));
//			context.write(text, new IntWritable(sum));
		}
	}
	
	public static class ReduceSideJoin_Reducer extends Reducer<Text, KeyValueArrayWritable, Text, Text>  {
		
		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
		      StringBuilder builder = new StringBuilder();
		      for (KeyValueArrayWritable array : values) {
		        for (KeyValue kv : (KeyValue[]) array.toArray()) {
		        	builder.append(""+
		        		  new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength()) + " "
		        		+ new String(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength()) + " "
		        		+ new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength()) + "\n");
		        }
		      }
			context.write(key, new Text(builder.toString()));
		}
		
	}
		    
}
