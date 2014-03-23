/**
 * Reduce Side Join Template for BSBM Benchmark
 * @date March 2013
 * @author Albert Haque
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.sql.Date;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class ReduceSideJoin {
	
	// Begin Query Information
	private static String ProductFeature1 = "bsbm-inst_ProductFeature1266";
	private static String ProductFeature2 = "bsbm-inst_ProductFeature1283";
	private static String ProductType = "bsbm-inst_ProductType131";
	private static int x = 0;
	// End Query Information
	
	private static byte[] CF_AS_BYTES = "o".getBytes();
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		// Zookeeper quorum is usually the same as the HBase master node
		String USAGE_MSG = "Arguments: <table name> <zookeeper quorum>";

		if (args == null || args.length != 2) {
			System.out.println("\n  You entered " + args.length + " arguments.");
			System.out.println("\n  " + USAGE_MSG);
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
			byte[] item2 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductType));
			if (item2 == null) { return; }
			String item2_str = new String(item2);
			if (!item2_str.equals("rdf_type")) { return; }
			
			// TriplePattern-3
			byte[] item3 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductFeature1));
			if (item3 == null) { return; }
			String item3_str = new String(item3);
			if (!item3_str.equals("bsbm-voc_productFeature")) { return; }
	
			// TriplePattern-4
			byte[] item4 = value.getValue(CF_AS_BYTES, Bytes.toBytes(ProductFeature2));
			if (item4 == null) { return; }
			String item4_str = new String(item4);
		    if (!item4_str.equals("bsbm-voc_productFeature")) { return; }

			// TriplePattern-6 - Since this is a literal, the predicate is the column name
			byte[] item6 = value.getValue(CF_AS_BYTES, Bytes.toBytes("bsbm-voc_productPropertyNumeric1"));
			if (item6 == null) { return; }
			int number6 = ByteBuffer.wrap(item6).getInt();
			if (number6 <= x) { return; }
			
			// Subject (Mapper Output: Key)
			text.set(new String(value.getRow()));
			
			// HBase row for that subject (Mapper Output: Value)
			List<KeyValue> entireRowAsList = value.list();
			KeyValue[] entireRow = new KeyValue[entireRowAsList.size()];
			// TODO: Reduce data sent across network by writing only columns that we know will be used
			for (int i = 0; i < entireRowAsList.size(); i++) {
				entireRow[i] = entireRowAsList.get(i);
			}
			
	    	context.write(text, new KeyValueArrayWritable(entireRow));
		}
	}
	
	public static class ReduceSideJoin_Reducer extends Reducer<Text, KeyValueArrayWritable, Text, Text>  {
		
		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
		      StringBuilder builder = new StringBuilder();
		      for (KeyValueArrayWritable array : values) {
		        for (KeyValue kv : (KeyValue[]) array.toArray()) {
		        	try {
						builder.append(keyValueToTripleString(kv));
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
		        	
		        }
		      }
			context.write(key, new Text(builder.toString()));
		}
		
		/*
		 * This method takes a KeyValue as an input and will return the String
		 * concatenation of the correct subject, predicate, and object. This method is
		 * necessary since the database stores most objects as columns except literals. In
		 * the case of literals, the column is the actual triple's predicate. Therefore
		 * we must output the correct String concatenation for both cases.
		 * 
		 * @param KeyValue kv - A single Hadoop KeyValue. Contains (key,value)=(subject,HBase cell)
		 * @return String result - String in the form: <Subject> <Predicate> <Object>
		 */
		public String keyValueToTripleString(KeyValue kv) throws IOException, ClassNotFoundException {
			StringBuilder builder = new StringBuilder();
			String result;
			DateTimeFormatter format1 = DateTimeFormat.forPattern("yyyy-MM-dd");
			DateTimeFormatter format2 = DateTimeFormat.forPattern("HH-mm-ss");
			/* If a literal then we need to:
        	 * 1. Use the column as the predicate
        	 * 2. Convert byte arrays to double/string/data format
        	 */
        	String columnName = new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
        	// Double/integer literals
        	if (columnName.contains("bsbm-voc_productPropertyNumeric") ||
        			columnName.equals("bsbm-voc_price") ||
        			columnName.equals("bsbm-voc_deliveryDays")) {
        		byte[] rawBytes = kv.getValue();
    			double number = ByteBuffer.wrap(rawBytes).getDouble();
    			result =  new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength()) + "\t"
    	        		+ new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength()) + "\t"
    	        		+ number + "\n";
    			return result;
        	}
        	// String literals
        	else if (columnName.contains("bsbm-voc_productPropertyTextual") || columnName.equals("dc_date")) {
        		result =  new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength()) + "\t"
    	        		+ new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength()) + "\t"
    	        		+ new String(kv.getValue()) + "\n";
    			return result;
        	}
        	// Date literal
        	else if (columnName.equals("dc_date")) {
        		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(kv.getValue()));
        		Date d = (Date) ois.readObject();
        		ois.close();
        		result =  new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength()) + "\t"
    	        		+ new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength()) + "\t"
    	        		+ d.toString() + "\n";
    			return result;
        	}
        	// DateTime
        	else if (columnName.equals("bsbm-voc_validTo") ||
        			 columnName.equals("bsbm-voc_validFrom")) { 
        		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(kv.getValue()));
        		DateTime d = (DateTime) ois.readObject();
        		ois.close();
        		result =  new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength()) + "\t"
    	        		+ new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength()) + "\t"
    	        		+ format1.print(d) + "T" + format2.print(d) + "\n";
    			return result;
        	}
        	// Object is not a literal
        	builder.append(""+
        		  new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength()) + "\t"
        		+ new String(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength()) + "\t"
        		+ new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength()) + "\n");
			return builder.toString();
		}
		
	}
		    
}
