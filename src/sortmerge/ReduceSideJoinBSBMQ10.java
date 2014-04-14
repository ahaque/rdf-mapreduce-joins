package sortmerge;

/**
 * Reduce Side Join BSBM Q10
 * @date April 2013
 * @author Albert Haque
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class ReduceSideJoinBSBMQ10 {
	
	// Begin Query Information
	private static String ProductXYZ = "bsbm-inst_dataFromProducer284/Product13895";
	private static String CurrentDateString = "2008-06-01T00-00-00";
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
		job1.setJobName("BSBM-Q10-ReduceSideJoin");
		job1.setJarByClass(ReduceSideJoinBSBMQ10.class);
		// Change caching and number of time stamps to speed up the scan
		scan1.setCaching(500);        
		scan1.setMaxVersions(200);
		scan1.setCacheBlocks(false);
		
		// Mapper settings
		TableMapReduceUtil.initTableMapperJob(
				args[0],		// input HBase table name
				scan1,			// Scan instance to control CF and attribute selection
				ReduceSideJoin_MapperStage1.class,	// mapper class
				Text.class,		// mapper output key
				KeyValueArrayWritable.class,  		// mapper output value
				job1);

		// Reducer settings
		//job1.setReducerClass(ReduceSideJoin_ReducerStage1.class);   
		job1.setReducerClass(SharedServices.ReduceSideJoin_Reducer.class);   
		
		job1.setOutputFormatClass(TextOutputFormat.class);
		//job1.setNumReduceTasks(1);  // Uncomment this if running into problems on 2+ node cluster
		FileOutputFormat.setOutputPath(job1, new Path("output/BSBMQ10"));

		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		  catch (InterruptedException e) { e.printStackTrace();}

		return job1;
	}

	
	public static class ReduceSideJoin_MapperStage1 extends TableMapper<Text, KeyValueArrayWritable> {
		
		private Text text = new Text();
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		/* BERLIN SPARQL BENHCMARK QUERY 10
		   ----------------------------------------
		SELECT DISTINCT ?offer ?price
		WHERE {
			[TP-01] ?offer bsbm:product %ProductXYZ% .
			[TP-02] ?offer bsbm:vendor ?vendor .
			[TP-03] ?offer dc:publisher ?vendor .
			[TP-04] ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
			[TP-05] ?offer bsbm:deliveryDays ?deliveryDays .
			[TP-06] FILTER (?deliveryDays <= 3)
			[TP-07] ?offer bsbm:price ?price .
			[TP-08] ?offer bsbm:validTo ?date .
			[TP-09] FILTER (?date > %currentDate% )
		}
		ORDER BY xsd:double(str(?price))
		LIMIT 10
		   ---------------------------------------*/
			String rowKey = new String(value.getRow());
			text.set(rowKey);
			
			ArrayList<KeyValue> keyValuesToTransmit = new ArrayList<KeyValue>();
			List<KeyValue> offerRow = value.list();
			
			boolean offerForProductXYZ = false;
			for (KeyValue kv : offerRow) {
				// TP-01
				if (Arrays.equals(kv.getValue(), "bsbm-voc_product".getBytes())) {
					if (!Arrays.equals(kv.getQualifier(), ProductXYZ.getBytes())) {
						return;
					}
					offerForProductXYZ = true;
				}
				// TP-05 and TP-06
				if (Arrays.equals(kv.getQualifier(), "bsbm-voc_deliveryDays".getBytes())) {
					int number = ByteBuffer.wrap(kv.getValue()).getInt();
					if (number > 3) {
						return;
					}
					keyValuesToTransmit.add(kv);
				}
				// TP-02 and TP-03
				if (Arrays.equals(kv.getValue(), "dc_publisher".getBytes())) {
					keyValuesToTransmit.add(kv);
				}
				// TP-07
				if (Arrays.equals(kv.getQualifier(), "bsbm-voc_price".getBytes())) {
					keyValuesToTransmit.add(kv);
				}
				// TP-08
				if (Arrays.equals(kv.getQualifier(), "bsbm-voc_validTo".getBytes())) {
					keyValuesToTransmit.add(kv);
				}
			}
			if (!offerForProductXYZ) {
				return;
			}
			context.write(text, new KeyValueArrayWritable(SharedServices.listToArray(keyValuesToTransmit)));
		}
	}
	
	// Output format:
	// Key: HBase Row Key (subject)
	// Value: All projected attributes for the row key (subject)
	public static class ReduceSideJoin_ReducerStage1 extends Reducer<Text, KeyValueArrayWritable, Text, Text>  {
		
	    HTable table;
	    long currentDate;
	    private static final Log LOG = LogFactory.getLog(ReduceSideJoin_ReducerStage1.class);

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	      Configuration conf = context.getConfiguration();
	      table = new HTable(conf, conf.get("scan.table"));
	      currentDate = SharedServices.dateTimeStringToLong(CurrentDateString);
	    }

		public void reduce(Text key, Iterable<KeyValueArrayWritable> values, Context context) throws IOException, InterruptedException {
			/* BERLIN SPARQL BENHCMARK QUERY 10
			   ----------------------------------------
		SELECT DISTINCT ?offer ?price
		WHERE {
			[TP-01] ?offer bsbm:product %ProductXYZ% .
			[TP-02] ?offer bsbm:vendor ?vendor .
			[TP-03] ?offer dc:publisher ?vendor .
			[TP-04] ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
			[TP-05] ?offer bsbm:deliveryDays ?deliveryDays .
			[TP-06] FILTER (?deliveryDays <= 3)
			[TP-07] ?offer bsbm:price ?price .
			[TP-08] ?offer bsbm:validTo ?date .
			[TP-09] FILTER (?date > %currentDate% )
		}
		ORDER BY xsd:double(str(?price))
		LIMIT 10
			   --------------------------------------- */
			
			List<KeyValue> finalKeyValues = new ArrayList<KeyValue>();
			
			// Find the keys for the vendor/publisher
			KeyValue kv_vendorURI = null;
			for (KeyValueArrayWritable array : values) {
				for (KeyValue kv : (KeyValue[]) array.toArray()) {
					if (Arrays.equals(kv.getValue(),"dc_publisher".getBytes())) {
						kv_vendorURI = kv;
						finalKeyValues.add(kv);
					} else if (Arrays.equals(kv.getQualifier(),"bsbm-voc_validTo".getBytes())) {
						// TP-09
						System.out.println(ByteBuffer.wrap(kv.getValue()).getLong() + " " + currentDate);
						LOG.info(ByteBuffer.wrap(kv.getValue()).getLong() + " " + currentDate);
						if (ByteBuffer.wrap(kv.getValue()).getLong() < currentDate) {
							return;
						}
						finalKeyValues.add(kv);
					} else {
						finalKeyValues.add(kv);
					}
				}
			}
			// TP-04
			Result vendorResult = table.get(new Get(kv_vendorURI.getQualifier()));
			byte[] vendorCountry = vendorResult.getValue(SharedServices.CF_AS_BYTES, "<http://downlode.org/rdf/iso-3166/countries#US>".getBytes());
			if (!Arrays.equals(vendorCountry,"bsbm-voc_country".getBytes())) {
				return;
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

			context.write(key, new Text(builder.toString()));
		}
	}	    
}