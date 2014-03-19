/**
 * Reduce Side Join Template for BSBM Benchmark
 * @date March 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
	
	// Query Information
	private static String ProductFeature1 = "bsbm-inst_ProductFeature3021";
	private static String ProductFeature2 = "bsbm-inst_ProductFeature685";
	private static String ProductType = "bsbm-inst_ProductType97";
	private static int x = 0;

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
	
	@SuppressWarnings("deprecation")
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
				IntWritable.class,  // mapper output value
				job);

		// Reducer settings
		job.setReducerClass(ReduceSideJoin_Reducer.class);    // reducer class
		job.setNumReduceTasks(1);    // at least one, adjust as required
	
		
		FileOutputFormat.setOutputPath(job, new Path("output/2014-03-13"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class ReduceSideJoin_Mapper extends TableMapper<Text, IntWritable> {
		
		private Text text = new Text();
		private final IntWritable ONE = new IntWritable(1);

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
			boolean skip = false;
			byte[] item2 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductType));
			if (item2 == null) { skip = true; }
			String item2_str = new String(item2);
			if (!item2_str.equals("rdf_type")) { skip = true; }
			
			// TP-3
			if (skip == false) {
			byte[] item3 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductFeature1));
			if (item3 == null) { skip = true;}
			String item3_str = new String(item3);
			if (!item3_str.equals("bsbm_productFeature")) { skip = true; }
			}
	
			// TP-4
			if (skip == false) {
			byte[] item4 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductFeature2));
			if (item4 == null) { skip = true; }
			String item4_str = new String(item4);
		    if (!item4_str.equals("bsbm_productFeature")) { skip = true; }
			}
			
			// TP-6 - Since this is a literal, the predicate is the column name
		    if (skip == false) {
			byte[] item6 = value.getValue(CF_AS_BYTES, Bytes.toBytes("bsbm_productPropertyNumeric1"));
			if (item6 == null) { skip = true; }
			int number6 = ByteBuffer.wrap(item6).getInt();
			if (number6 <= x) { skip = true; }
		    }
			
			text.set(new String(value.getRow()));
						
			// Tuple is the project part of the SPARQL query
//			Text[] tuple = new Text[2];
//		    			
//			List<Cell> allKeyValues = value.listCells();
//			for (Cell kv : allKeyValues) {
//				if (new String(kv.getValueArray()).equals("rdf_type")) {
//					// Even numbers are predicates, odd numbers are objects
//					tuple[0] = new Text(kv.getValueArray());
//					tuple[1] = new Text(kv.getQualifierArray());
//					break;
//				}
//			}
//			// Mapper Output Key: Row key/subject
//		    text.set(new String(value.getRow()));		   
//		    // Mapper Output Value: Predicate and Object
//	    	context.write(text, new TextArrayWritable(tuple));
			context.write(text, ONE);
		}
	}
	
	public static class ReduceSideJoin_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//			StringBuilder build = new StringBuilder();
//			for (TextArrayWritable array : values) {
//				Text[] tuple = (Text[]) array.toArray();
//		        build.append(tuple[0] + " " + tuple[1]);
//		      }
//		      context.write(key, new Text(build.toString()));
			int i = 0;
			for (IntWritable val : values) {
			i += val.get();
			}
			context.write(key, new IntWritable(i));
		}
		
	}
		    
}
