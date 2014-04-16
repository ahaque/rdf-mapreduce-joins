package repartition;

/**
 * Repartition Join BSBM Q4
 * @date April 2013
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

import sortmerge.KeyValueArrayWritable;
import sortmerge.SharedServices;


public class RepartitionJoinQ4 {
	
	// Begin Query Information
	private static String ProductType = "bsbm-inst_ProductType230";
	private static String ProductFeature1 = "bsbm-inst_ProductFeature39";
	private static String ProductFeature2 = "bsbm-inst_ProductFeature41";
	private static String ProductFeature3 = "bsbm-inst_ProductFeature41";
	private static int x = 0;
	private static int y = 5000;
	private static String[] ProjectedVariables = {"rdfs_label", "bsbm-voc_productPropertyTextual1"};
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
		job.setJobName("BSBM-Q4-RepartitionJoin");
		job.setJarByClass(RepartitionJoinQ4.class);
		// Change caching to speed up the scan
		scan.setCaching(500);        
		scan.setMaxVersions(200);
		scan.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],        // input HBase table name
				scan,             // Scan instance to control CF and attribute selection
				RepartitionMapper.class,   // mapper
				CompositeKeyWritable.class,         // mapper output key
				KeyValueArrayWritable.class,  // mapper output value
				job);
		
		// Repartition settings
		job.setPartitionerClass(CompositePartitioner.class);
		job.setSortComparatorClass(CompositeSortComparator.class);
		job.setGroupingComparatorClass(CompositeGroupingComparator.class);

		// Reducer settings
		job.setReducerClass(SharedServices.RepartitionJoin_Reducer.class);    // reducer class
		job.setNumReduceTasks(1);    // at least one, adjust as required
	
		FileOutputFormat.setOutputPath(job, new Path("output/BSBMQ4"));

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job;
	}
	
	
	public static class RepartitionMapper extends TableMapper<CompositeKeyWritable, KeyValueArrayWritable> {

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* BERLIN SPARQL BENHCMARK QUERY 3
		   ----------------------------------------
		SELECT DISTINCT ?product ?label ?propertyTextual
		WHERE {
			{ 
			[TP-01]	?product rdfs:label ?label .
			[TP-02]	?product rdf:type %ProductType% .
			[TP-03]	?product bsbm:productFeature %ProductFeature1% .
			[TP-04]	?product bsbm:productFeature %ProductFeature2% .
			[TP-05]	?product bsbm:productPropertyTextual1 ?propertyTextual .
			[TP-06]	?product bsbm:productPropertyNumeric1 ?p1 .
			[TP-07]	FILTER ( ?p1 > %x% )
			} UNION {
			[TP-08]	?product rdfs:label ?label .
			[TP-09]	?product rdf:type %ProductType% .
			[TP-10]	?product bsbm:productFeature %ProductFeature1% .
			[TP-11]	?product bsbm:productFeature %ProductFeature3% .
			[TP-12]	?product bsbm:productPropertyTextual1 ?propertyTextual .
			[TP-13]	?product bsbm:productPropertyNumeric2 ?p2 .
			[TP-14]	FILTER ( ?p2> %y% ) 
			} 
		}
		ORDER BY ?label
		OFFSET 5
		LIMIT 10
		   ---------------------------------------
		 */
			// Triple patterns within both sides of union
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

			if (!isPartOfFirstUnion(value) && !isPartOfSecondUnion(value)) {
				return;
			}
		
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
	    	context.write(new CompositeKeyWritable(new String(value.getRow()),1),
	    			new KeyValueArrayWritable(entireRow));
		}
		
		public boolean isPartOfFirstUnion(Result value) {
			/* FIRST UNION
			[TP-04]	?product bsbm:productFeature %ProductFeature2% .
			[TP-06]	?product bsbm:productPropertyNumeric1 ?p1 .
			[TP-07]	FILTER ( ?p1 > %x% )
			 */
			// TP-04
			byte[] item4 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductFeature2));
			if (item4 == null) { return false; }
			String item4_str = new String(item4);
			if (!item4_str.equals("bsbm-voc_productFeature")) { return false; }
			
			// TP-06-07
			byte[] item6 = value.getValue(CF_AS_BYTES, Bytes.toBytes("bsbm-voc_productPropertyNumeric1"));
			if (item6 == null) { return false; }
			int number6 = ByteBuffer.wrap(item6).getInt();
			if (number6 <= x) { return false; }
			
			return true;
		}
		
		public boolean isPartOfSecondUnion(Result value) {
			/* FIRST UNION
			[TP-11]	?product bsbm:productFeature %ProductFeature3% .
			[TP-13]	?product bsbm:productPropertyNumeric2 ?p2 .
			[TP-14]	FILTER ( ?p2> %y% ) 
			 */
			// TP-11
			byte[] item4 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductFeature3));
			if (item4 == null) { return false; }
			String item4_str = new String(item4);
			if (!item4_str.equals("bsbm-voc_productFeature")) { return false; }
			
			// TP-13-14
			byte[] item6 = value.getValue(CF_AS_BYTES, Bytes.toBytes("bsbm-voc_productPropertyNumeric2"));
			if (item6 == null) { return false; }
			int number6 = ByteBuffer.wrap(item6).getInt();
			if (number6 <= y) { return false; }
			
			return true;
		}
		
	}
		    
}
