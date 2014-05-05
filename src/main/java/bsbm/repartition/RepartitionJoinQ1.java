package bsbm.repartition;

/**
 * Repartition Join BSBM Q1
 * @date April 2014
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bsbm.sortmerge.SharedServices;
import bsbm.sortmerge.KeyValueArrayWritable;

public class RepartitionJoinQ1 {
	
	// Begin Query Information
	private static String ProductFeature1 = "bsbm-inst_ProductFeature35";
	private static String ProductFeature2 = "bsbm-inst_ProductFeature31";
	private static String ProductType = "bsbm-inst_ProductType183";
	private static int x = 0;
	private static String[] ProjectedVariables = {"rdfs_label"};
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
		Job job = new Job(hConf);
		job.setJobName("BSBM-Q1-RepartitionJoin");
		job.setJarByClass(RepartitionJoinQ1.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
		scan.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],        // input HBase table name
				scan,           // Scan instance to control CF and attribute selection
				RepartitionMapper.class,     // mapper
				CompositeKeyWritable.class,   // mapper output key
				KeyValueArrayWritable.class,  // mapper output value
				job);
		
		// Repartition settings
		job.setPartitionerClass(CompositePartitioner.class);
		job.setSortComparatorClass(CompositeSortComparator.class);
		job.setGroupingComparatorClass(CompositeGroupingComparator.class);
		
		// Reducer settings
		job.setReducerClass(SharedServices.RepartitionJoin_Reducer.class);
		job.setNumReduceTasks(1);
	
		FileOutputFormat.setOutputPath(job, new Path("output/BSBMQ1"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class RepartitionMapper extends TableMapper<CompositeKeyWritable, KeyValueArrayWritable> {
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* BERLIN SPARQL BENHCMARK QUERY 1
		   ----------------------------------------
		   SELECT DISTINCT ?product ?label
			WHERE { 
	 [TriplePattern-1] ?product rdfs:label ?label .  
	 [TriplePattern-2] ?product a %ProductType% .
	 [TriplePattern-3] ?product bsbm:productFeature %ProductFeature1% . 
	 [TriplePattern-4] ?product bsbm:productFeature %ProductFeature2% . 
	 [TriplePattern-5] ?product bsbm:productPropertyNumeric1 ?value1 . 
	 [TriplePattern-6] FILTER (?value1 > %x%) 
				}
			ORDER BY ?label
			LIMIT 10
		   ---------------------------------------
		 */
			// TriplePattern-2
			byte[] item2 = value.getValue(SharedServices.CF_AS_BYTES, Bytes.toBytes(ProductType));
			if (item2 == null) { return; }
			String item2_str = new String(item2);
			if (!item2_str.equals("rdf_type")) { return; }
			
			// TriplePattern-3
			byte[] item3 = value.getValue(SharedServices.CF_AS_BYTES, Bytes.toBytes(ProductFeature1));
			if (item3 == null) { return; }
			String item3_str = new String(item3);
			if (!item3_str.equals("bsbm-voc_productFeature")) { return; }
	
			// TriplePattern-4
			byte[] item4 = value.getValue(SharedServices.CF_AS_BYTES, Bytes.toBytes(ProductFeature2));
			if (item4 == null) { return; }
			String item4_str = new String(item4);
		    if (!item4_str.equals("bsbm-voc_productFeature")) { return; }

			// TriplePattern-6 - Since this is a literal, the predicate is the column name
			byte[] item6 = value.getValue(SharedServices.CF_AS_BYTES, Bytes.toBytes("bsbm-voc_productPropertyNumeric1"));
			if (item6 == null) { return; }
			int number6 = ByteBuffer.wrap(item6).getInt();
			if (number6 <= x) { return; }
			
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
	    	context.write(new CompositeKeyWritable(new String(value.getRow()),1), new KeyValueArrayWritable(entireRow));
		}
	}
}
