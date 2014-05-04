package bsbm.repartition;

/**
 * Repartition Join BSBM Q7
 * @date April 2014
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bsbm.sortmerge.KeyValueArrayWritable;
import bsbm.sortmerge.SharedServices;

public class RepartitionJoinQ7 {
	
	// Begin Query Information
	private static String ProductXYZ = "bsbm-inst_dataFromProducer105/Product4956";
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
	
	// MapReduce Stage-1 Job
	public static Job startJob_Stage1(String[] args, Configuration hConf) throws IOException {
		
		// args[0] = hbase table name
		// args[1] = zookeeper

	    Scan scan1 = new Scan();		
		Job job1 = new Job(hConf);
		job1.setJobName("BSBM-Q7-RepartitionJoin");
		job1.setJarByClass(RepartitionJoinQ7.class);
		// Change caching and number of time stamps to speed up the scan
		scan1.setCaching(500);        
		scan1.setMaxVersions(200);
		scan1.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],		// input HBase table name
				scan1,			// Scan instance to control CF and attribute selection
				RepartitionMapper.class,	// mapper class
				CompositeKeyWritable.class,		// mapper output key
				KeyValueArrayWritable.class,  		// mapper output value
				job1);

		// Reducer settings
		job1.setReducerClass(RepartitionReducer.class);   
		
		// Repartition settings
		job1.setPartitionerClass(CompositePartitioner.class);
		job1.setSortComparatorClass(CompositeSortComparator.class);
		job1.setGroupingComparatorClass(CompositeGroupingComparator.class);
		
		job1.setOutputFormatClass(TextOutputFormat.class);
		//job1.setNumReduceTasks(1);  // Uncomment this if running into problems on 2+ node cluster
		FileOutputFormat.setOutputPath(job1, new Path("output/BSBMQ7"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}

	
	public static class RepartitionMapper extends TableMapper<CompositeKeyWritable, KeyValueArrayWritable> {
		
	    HTable table;

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	      Configuration conf = context.getConfiguration();
	      table = new HTable(conf, conf.get("scan.table"));
	    }
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* BERLIN SPARQL BENHCMARK QUERY 7
		   ----------------------------------------
			SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle 
					?reviewer ?revName ?rating1 ?rating2
			WHERE { 
					[TP-00] %ProductXYZ% rdfs:label ?productLabel .
			[OPT-01] OPTIONAL {
					[TP-01] ?offer bsbm:product %ProductXYZ% .
					[TP-02] ?offer bsbm:price ?price .
					[TP-03] ?offer bsbm:vendor ?vendor .
					[TP-04] ?vendor rdfs:label ?vendorTitle .
					[TP-05] ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> .
					[TP-06] ?offer dc:publisher ?vendor . 
					[TP-07] ?offer bsbm:validTo ?date .
					[TP-08] FILTER (?date > %currentDate% )
				}
			[OPT-02] OPTIONAL {
					[TP-09] ?review bsbm:reviewFor %ProductXYZ% .
					[TP-10] ?review rev:reviewer ?reviewer .
					[TP-11] ?reviewer foaf:name ?revName .
					[TP-12] ?review dc:title ?revTitle .
					[TP-13] OPTIONAL { ?review bsbm:rating1 ?rating1 . }
					[TP-14] OPTIONAL { ?review bsbm:rating2 ?rating2 . } 
				}
			} ---------------------------------------*/
			String rowKey = new String(value.getRow());
			
			ArrayList<KeyValue> keyValuesToTransmit = new ArrayList<KeyValue>();
			List<KeyValue> rowData = value.list();
			
			// TP-01
			byte[] predicate = value.getValue(SharedServices.CF_AS_BYTES, ProductXYZ.getBytes());
			// If this row has no relationship to the product, then we can end here
			if (predicate == null) {
				return;
			}
			int requiredCount = 0;
			// If the row is an offer
			if (Arrays.equals(predicate, "bsbm-voc_product".getBytes())) {
				for (KeyValue kv : rowData) {
					// TP-01
					if (Arrays.equals(kv.getValue(), "bsbm-voc_product".getBytes())) {
						keyValuesToTransmit.add(kv);
						requiredCount++;
					}
					// TP-02
					else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_price".getBytes())) {
						keyValuesToTransmit.add(kv);
						requiredCount++;
					}
					// TP-03 and TP-06
					else if (Arrays.equals(kv.getValue(), "dc_publisher".getBytes())) {
						keyValuesToTransmit.add(kv);
						requiredCount++;
					}
					// TP-07
					else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_validTo".getBytes())) {
						keyValuesToTransmit.add(kv);
						requiredCount++;
					}
				}
				// If we're missing a required column, discard this row
				if (requiredCount < 4) {
					return;
				} // Otherwise this row is a valid offer
				context.write(new CompositeKeyWritable(rowKey, 1), new KeyValueArrayWritable(SharedServices.listToArray(keyValuesToTransmit)));
				return;
			}
			// OPT-02, If the row is a review
			if (Arrays.equals(predicate, "bsbm-voc_reviewFor".getBytes())) {
				for (KeyValue kv : rowData) {
					// TP-09
					if (Arrays.equals(kv.getValue(), "bsbm-voc_reviewFor".getBytes())) {
						keyValuesToTransmit.add(kv);
						requiredCount++;
					}
					// TP-10
					else if (Arrays.equals(kv.getValue(), "rev_reviewer".getBytes())) {
						keyValuesToTransmit.add(kv);
						requiredCount++;
					}
					// TP-12
					else if (Arrays.equals(kv.getQualifier(), "dc_title".getBytes())) {
						keyValuesToTransmit.add(kv);
						requiredCount++;
					}
					// TP-13 and TP-14
					else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_rating1".getBytes())) {
						keyValuesToTransmit.add(kv);
					}
					else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_rating2".getBytes())) {
						keyValuesToTransmit.add(kv);
					}
				}
				// If we're missing a required column, discard this row
				if (requiredCount < 3) {
					return;
				} // Otherwise this row is a valid offer
				context.write(new CompositeKeyWritable(rowKey, 2), new KeyValueArrayWritable(SharedServices.listToArray(keyValuesToTransmit)));
				return;
			}
		}
	}
	

	public static class RepartitionReducer extends Reducer<CompositeKeyWritable, KeyValueArrayWritable, Text, Text>  {
		
	    HTable table;

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	      Configuration conf = context.getConfiguration();
	      table = new HTable(conf, conf.get("scan.table"));
	    }

		public void reduce(CompositeKeyWritable key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
			
			// TableTag = 1 => Review
			// TableTag = 2 => Offer
			List<KeyValue> finalKeyValues = new ArrayList<KeyValue>();
			// TP-00
			Result productResult = table.get(new Get(ProductXYZ.getBytes()));
			for (KeyValue kv : productResult.list()) {
				if (Arrays.equals(kv.getQualifier(), "rdfs_label".getBytes())) {
					finalKeyValues.add(kv);
					break;
				}
			}
			
			boolean isReview = false;
			boolean rowTypeAssigned = false;
			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (!rowTypeAssigned && new String(kv.getRow()).contains("Review")) {
						isReview = true;
						rowTypeAssigned = true;
					}
					finalKeyValues.add(kv);
				}
			}
			
			if (isReview) {
				Result reviewerResult = null;
				for (KeyValue kv : finalKeyValues) {
					if (Arrays.equals(kv.getValue(), "rev_reviewer".getBytes())) {
						reviewerResult = table.get(new Get(kv.getQualifier()));
						break;
					}
				}
				// TP-11
				for (KeyValue kv : reviewerResult.list()) {
					if (Arrays.equals(kv.getQualifier(), "foaf_name".getBytes())) {
						finalKeyValues.add(kv);
					}
				}
			} else {
				Result vendorResult = null;
				for (KeyValue kv : finalKeyValues) {
					if (Arrays.equals(kv.getValue(), "dc_publisher".getBytes())) {
						vendorResult = table.get(new Get(kv.getQualifier()));
						break;
					}
				}
				
				// TP-05
				for (KeyValue kv : vendorResult.list()) {
					// TP-04
					if (Arrays.equals(kv.getQualifier(), "rdfs_label".getBytes())) {
						finalKeyValues.add(kv);
					} else if (Arrays.equals(kv.getValue(), "bsbm-voc_country".getBytes())) {
						if (!Arrays.equals(kv.getQualifier(), "<http://downlode.org/rdf/iso-3166/countries#DE>".getBytes())) {
							return;
						}
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
	        	builder.append(triple[0] + "\t" + triple[1] + "\t" + triple[2] +"\n");
	        }

			context.write(new Text(key.getValue()), new Text(builder.toString()));
		}
	}	    
}