package repartition;

/**
 * Repartition Join BSBM Q12
 * @date April 2013
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

import sortmerge.KeyValueArrayWritable;
import sortmerge.SharedServices;

public class RepartitionJoinQ12 {
	
	// Begin Query Information
	private static String OfferXYZ = "bsbm-inst_dataFromVendor215/Offer421332";
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

		/*
		 * MapReduce Stage-1 Job
		 * Retrieve a list of subjects and their attributes
		 */
	    Scan scan1 = new Scan();		
		Job job1 = new Job(hConf);
		job1.setJobName("BSBM-Q12-RepartitionJoin");
		job1.setJarByClass(RepartitionJoinQ12.class);
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

		// Repartition settings
		job1.setPartitionerClass(CompositePartitioner.class);
		job1.setSortComparatorClass(CompositeSortComparator.class);
		job1.setGroupingComparatorClass(CompositeGroupingComparator.class);
		
		// Reducer settings
		job1.setReducerClass(RepartitionReducer.class);   
		job1.setOutputFormatClass(TextOutputFormat.class);
		//job1.setNumReduceTasks(1);  // Uncomment this if running into problems on 2+ node cluster
		FileOutputFormat.setOutputPath(job1, new Path("output/BSBMQ12"));

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
		/* BERLIN SPARQL BENHCMARK QUERY 12
		   ----------------------------------------
		CONSTRUCT { %OfferXYZ% bsbm-export:product ?productURI .
		 %OfferXYZ% bsbm-export:productlabel ?productlabel .
		 %OfferXYZ% bsbm-export:vendor ?vendorname .
		 %OfferXYZ% bsbm-export:vendorhomepage ?vendorhomepage . 
		 %OfferXYZ% bsbm-export:offerURL ?offerURL .
		 %OfferXYZ% bsbm-export:price ?price .
		 %OfferXYZ% bsbm-export:deliveryDays ?deliveryDays .
		 %OfferXYZ% bsbm-export:validuntil ?validTo } 
		WHERE {
			[TP-01] %OfferXYZ% bsbm:product ?productURI .
			[TP-02] ?productURI rdfs:label ?productlabel .
			[TP-03] %OfferXYZ% bsbm:vendor ?vendorURI .
			[TP-04] ?vendorURI rdfs:label ?vendorname .
			[TP-05] ?vendorURI foaf:homepage ?vendorhomepage .
			[TP-06] %OfferXYZ% bsbm:offerWebpage ?offerURL .
			[TP-07] %OfferXYZ% bsbm:price ?price .
			[TP-08] %OfferXYZ% bsbm:deliveryDays ?deliveryDays .
			[TP-09] %OfferXYZ% bsbm:validTo ?validTo
		}
		   ---------------------------------------*/
			String rowKey = new String(value.getRow());
			
			if (!rowKey.equals(OfferXYZ)) {
				return;
			}
			ArrayList<KeyValue> keyValuesToTransmit = new ArrayList<KeyValue>();
			List<KeyValue> productRow = value.list();
			// TP-06, TP-07, TP-08, TP-09
			boolean found1 = false;
			boolean found2 = false;
			boolean found3 = false;
			boolean found4 = false;

			for (KeyValue kv : productRow) {
				if (Arrays.equals(kv.getValue(), "bsbm-voc_offerWebpage".getBytes())) {
					keyValuesToTransmit.add(kv);
					found1 = true;
				} else if (Arrays.equals(kv.getValue(), "bsbm-voc_product".getBytes())) {
					keyValuesToTransmit.add(kv);
				} else if (Arrays.equals(kv.getValue(), "dc_publisher".getBytes())) {
					keyValuesToTransmit.add(kv);
				} else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_price".getBytes())) {
					keyValuesToTransmit.add(kv);
					found2 = true;
				} else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_validTo".getBytes())) {
					keyValuesToTransmit.add(kv);
					found3 = true;
				} else if (Arrays.equals(kv.getQualifier(), "bsbm-voc_deliveryDays".getBytes())) {
					keyValuesToTransmit.add(kv);
					found4 = true;
				}
			}
			if (!found1 || !found2 || !found3 || !found4) {
				return;
			}
			
			/*
			 * [TP-01] %OfferXYZ% bsbm:product ?productURI .
			   [TP-02] ?productURI rdfs:label ?productlabel .
			 */
			context.write(new CompositeKeyWritable(rowKey, 1),
					new KeyValueArrayWritable(SharedServices.listToArray(keyValuesToTransmit)));
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class RepartitionReducer extends Reducer<CompositeKeyWritable, KeyValueArrayWritable, Text, Text>  {
		
	    HTable table;

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	      Configuration conf = context.getConfiguration();
	      table = new HTable(conf, conf.get("scan.table"));
	    }

		public void reduce(CompositeKeyWritable key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
			/* BERLIN SPARQL BENHCMARK QUERY 12
			   ----------------------------------------
		CONSTRUCT { %OfferXYZ% bsbm-export:product ?productURI .
		 %OfferXYZ% bsbm-export:productlabel ?productlabel .
		 %OfferXYZ% bsbm-export:vendor ?vendorname .
		 %OfferXYZ% bsbm-export:vendorhomepage ?vendorhomepage . 
		 %OfferXYZ% bsbm-export:offerURL ?offerURL .
		 %OfferXYZ% bsbm-export:price ?price .
		 %OfferXYZ% bsbm-export:deliveryDays ?deliveryDays .
		 %OfferXYZ% bsbm-export:validuntil ?validTo } 
		WHERE {
			[TP-01] %OfferXYZ% bsbm:product ?productURI .
			[TP-02] ?productURI rdfs:label ?productlabel .
			[TP-03] %OfferXYZ% bsbm:vendor ?vendorURI .
			[TP-04] ?vendorURI rdfs:label ?vendorname .
			[TP-05] ?vendorURI foaf:homepage ?vendorhomepage .
			[TP-06] %OfferXYZ% bsbm:offerWebpage ?offerURL .
			[TP-07] %OfferXYZ% bsbm:price ?price .
			[TP-08] %OfferXYZ% bsbm:deliveryDays ?deliveryDays .
			[TP-09] %OfferXYZ% bsbm:validTo ?validTo
		}
			   --------------------------------------- */
			
			List<KeyValue> finalKeyValues = new ArrayList<KeyValue>();
			
			// Find the keys for the product and the publisher/vendor
			KeyValue kv_vendorURI = null;
			KeyValue kv_productURI = null;
			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (Arrays.equals(kv.getValue(),"bsbm-voc_product".getBytes())) {
						kv_productURI = kv;
						finalKeyValues.add(kv); // Since productURI is part of construct section of query
					} else if (Arrays.equals(kv.getValue(),"dc_publisher".getBytes())) {
						kv_vendorURI = kv;
					} else {
						finalKeyValues.add(kv);
					}
				}
			}
			
			/*
			[TP-03] %OfferXYZ% bsbm:vendor ?vendorURI .
			[TP-04] ?vendorURI rdfs:label ?vendorname .
			[TP-05] ?vendorURI foaf:homepage ?vendorhomepage .
			 */
			Result vendorResult = table.get(new Get(kv_vendorURI.getQualifier()));
			for (KeyValue kv : vendorResult.list()) {
				// TP-04
				if (Arrays.equals(kv.getQualifier(),"rdfs_label".getBytes())) {
					finalKeyValues.add(kv);
				}
				// TP-05
				else if (Arrays.equals(kv.getValue(),"foaf_homepage".getBytes())) {
					finalKeyValues.add(kv);
				}
			}
			
			/*
			 [TP-01] %OfferXYZ% bsbm:product ?productURI .
			[TP-02] ?productURI rdfs:label ?productlabel .
			 */
			Result productResult = table.get(new Get(kv_productURI.getQualifier()));
			
			for (KeyValue kv : productResult.list()) {
				// TP-02
				if (Arrays.equals(kv.getQualifier(),"rdfs_label".getBytes())) {
					finalKeyValues.add(kv);
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
