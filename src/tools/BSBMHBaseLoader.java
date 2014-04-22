package tools;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author David Alves, Albert Haque
 * @date April 2014
 */
public class BSBMHBaseLoader extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	
	// Used for Split-Key Generation at the bottom of this file
	public static byte[][] splitKeys = {};

  private long timestamp = 0;


  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    for (BSBMDataSetProcessor.Triple triple : BSBMDataSetProcessor.process(value.toString())) {

      byte[] subject = toBytes(triple.subject);

      Put put = new Put(subject);

      // if the triple is not a valu triple we store the object as the col name and the predicate as the col
      // value so that we can do bloom filtered s-o joins
      if (!triple.isValueTriple()) {
        put.add(toBytes(BSBMDataSetProcessor.COLUMN_FAMILY),
          toBytes(triple.object),
          timestamp++,
          toBytes(triple.predicate));
        // if it is a value (like an int or a data) we wouldn't join on it anyway so store as usual
      } else {
        put.add(toBytes(BSBMDataSetProcessor.COLUMN_FAMILY),
          toBytes(triple.predicate),
          timestamp++,
          triple.value);
      }

      ImmutableBytesWritable ibKey = new ImmutableBytesWritable(subject);
      context.write(ibKey, put);
    }
  }

  public static void main(String[] args) throws Exception {

    String USAGE_MSG = "  Arguments: <hbase table name> <zk quorum> <input path> <output path> <number of slave nodes> <dataset size {10,100,1000}>";

    if (args == null || args.length != 6) {
      System.out.println(USAGE_MSG);
      System.exit(0);
    }
    
    // Set the split keys accordingly
    int numNodes = -1;
    int datasetSize = -1;
    try {
    	numNodes = Integer.parseInt(args[4]);
    	datasetSize = Integer.parseInt(args[5]);
    } catch (NumberFormatException e) {
    	System.out.println(USAGE_MSG);
    	System.out.println("  Number of nodes and dataset size must be an integer");
    	System.exit(0);
    }
    if (!((numNodes & -numNodes) == numNodes)) {
    	System.out.println("  Number of nodes must be a power of 2");
    	System.exit(0);
    }
    
    setSplitKeys(numNodes, datasetSize);
    Job job = createMRJob(args);
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
  
  public static void setSplitKeys(int numNodes, int datasetSize) {
	  byte[][] workingSetArray = null;
	  // Figure out which set of split keys we'll need
	  switch (datasetSize) {
		  case 10: workingSetArray = splitKeys10m; break;
		  case 100: workingSetArray = splitKeys100m; break;
		  case 1000: workingSetArray = splitKeys1000m; break;
	  }
	  
	  // Select the keys that evenly splits the data across our nodes
	  List<byte[]> workingSetList = new ArrayList<byte[]>();
	  for (int i = 0; i < MAX_NODES; ) {
		  i += MAX_NODES/numNodes;
		  if (i > workingSetArray.length) {
			  break;
		  }
		  workingSetList.add(workingSetArray[i-1]);
	  }
	  // Add the keys to the SPLITKEYS array
	  splitKeys = new byte[workingSetList.size()][];
	  for (int i = 0; i < workingSetList.size(); i++) {
		  splitKeys[i] = workingSetList.get(i);
		  //System.out.println(new String(splitKeys[i]));
	  }
  }

  public static Job createMRJob(String[] args) throws Exception {

    String hbaseTable = args[0];
    HTable hTable;

    // Arguments: <hbase table name> <zk quorum> <input path> <output path>
    Configuration conf = new Configuration();
    conf.set("hbase.mapred.outputtable", args[0]);
    conf.set("hbase.hstore.blockingStoreFiles", "25");
    conf.set("hbase.hregion.memstore.block.multiplier", "8");
    conf.set("hbase.regionserver.handler.count", "50");
    conf.set("hbase.regions.percheckin", "30");
    conf.set("hbase.regionserver.globalMemcache.upperLimit", "0.3");
    conf.set("hbase.regionserver.globalMemcache.lowerLimit", "0.15");
    conf.set("hbase.hregion.max.filesize", "10737418240");

    Configuration hConf = HBaseConfiguration.create(conf);
    hConf.set("hbase.zookeeper.quorum", args[1]);
    hConf.set("hbase.zookeeper.property.clientPort", "2181");

    try {
      @SuppressWarnings("resource")
	HBaseAdmin admin = new HBaseAdmin(hConf);
      if (!admin.tableExists(hbaseTable)) {
        System.out.println("Could not find HBase table " + hbaseTable + ", creating now");
        HTableDescriptor desc = new HTableDescriptor();
        desc.setName(hbaseTable.getBytes());
        HColumnDescriptor colDesc = new HColumnDescriptor(BSBMDataSetProcessor.COLUMN_FAMILY);
        colDesc.setBloomFilterType(BloomType.ROWCOL);
        //colDesc.setCacheBloomsOnWrite(true);
        colDesc.setMaxVersions(200);
        desc.addFamily(colDesc);
        admin.createTable(desc, splitKeys);
      }
      hTable = new HTable(hConf, hbaseTable);
      System.out.println("Table created successfully");
    } catch (Exception e) {
      throw new RuntimeException("Error while accessing hbase....");
    }


    @SuppressWarnings("deprecation")
	Job job = new Job(conf);
    job.setJobName("BSBMToHBaseLoader");
    job.setJarByClass(BSBMHBaseLoader.class);
    job.setMapperClass(BSBMHBaseLoader.class);

    TextInputFormat.setInputPaths(job, new Path(args[2]));
    job.setInputFormatClass(TextInputFormat.class);

    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);

    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);
    job.setOutputFormatClass(HFileOutputFormat.class);

    job.setReducerClass(PutSortReducer.class);
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    HFileOutputFormat.configureIncrementalLoad(job, hTable);

    return job;
  }

	// The maximum number of nodes is how many split keys were generated
	// We only have enough keys below to support 64 nodes. If you need more
	// nodes
	// You need to re-generate the keys using Hadoop's InputSampler
	private static int MAX_NODES = 64;
	public static byte[][] splitKeys10m = {
			// Generated from Hadoop's input sampler
		Bytes.toBytes("bsbm-inst_ProductFeature5758"),
		Bytes.toBytes("bsbm-inst_dataFromProducer269/Product13176"),
		Bytes.toBytes("bsbm-inst_dataFromProducer538/Product26554"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite1/Review9270"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite11/Review104500"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite12/Review115202"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite13/Reviewer6537"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite15/Review142722"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite16/Review156471"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite16/Review170977"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite17/Review184110"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite19/Review197742"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite20/Review201708"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite21/Review215844"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite22/Review229447"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite23/Reviewer12361"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite25/Review257642"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite27/Review270803"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite28/Review284780"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite4/Review29295"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite5/Review43703"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite6/Review57681"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite8/Review71496"),
		Bytes.toBytes("bsbm-inst_dataFromRatingSite9/Review85808"),
		Bytes.toBytes("bsbm-inst_dataFromVendor102/Offer209786"),
		Bytes.toBytes("bsbm-inst_dataFromVendor110/Offer223110"),
		Bytes.toBytes("bsbm-inst_dataFromVendor12/Offer19846"),
		Bytes.toBytes("bsbm-inst_dataFromVendor125/Offer249903"),
		Bytes.toBytes("bsbm-inst_dataFromVendor131/Offer262202"),
		Bytes.toBytes("bsbm-inst_dataFromVendor14/Offer23846"),
		Bytes.toBytes("bsbm-inst_dataFromVendor145/Offer289442"),
		Bytes.toBytes("bsbm-inst_dataFromVendor151/Offer301799"),
		Bytes.toBytes("bsbm-inst_dataFromVendor16/Offer28969"),
		Bytes.toBytes("bsbm-inst_dataFromVendor165/Offer328670"),
		Bytes.toBytes("bsbm-inst_dataFromVendor171/Offer341208"),
		Bytes.toBytes("bsbm-inst_dataFromVendor178/Offer355872"),
		Bytes.toBytes("bsbm-inst_dataFromVendor186/Offer369107"),
		Bytes.toBytes("bsbm-inst_dataFromVendor194/Offer383078"),
		Bytes.toBytes("bsbm-inst_dataFromVendor201/Offer394226"),
		Bytes.toBytes("bsbm-inst_dataFromVendor208/Offer409033"),
		Bytes.toBytes("bsbm-inst_dataFromVendor215/Offer421256"),
		Bytes.toBytes("bsbm-inst_dataFromVendor222/Offer434425"),
		Bytes.toBytes("bsbm-inst_dataFromVendor229/Offer448735"),
		Bytes.toBytes("bsbm-inst_dataFromVendor235/Offer460482"),
		Bytes.toBytes("bsbm-inst_dataFromVendor241/Offer472278"),
		Bytes.toBytes("bsbm-inst_dataFromVendor247/Offer487292"),
		Bytes.toBytes("bsbm-inst_dataFromVendor254/Offer500279"),
		Bytes.toBytes("bsbm-inst_dataFromVendor261/Offer512763"),
		Bytes.toBytes("bsbm-inst_dataFromVendor267/Offer526862"),
		Bytes.toBytes("bsbm-inst_dataFromVendor273/Offer538737"),
		Bytes.toBytes("bsbm-inst_dataFromVendor281/Offer551364"),
		Bytes.toBytes("bsbm-inst_dataFromVendor288/Offer566080"),
		Bytes.toBytes("bsbm-inst_dataFromVendor294/Offer578729"),
		Bytes.toBytes("bsbm-inst_dataFromVendor35/Offer67378"),
		Bytes.toBytes("bsbm-inst_dataFromVendor40/Offer79990"),
		Bytes.toBytes("bsbm-inst_dataFromVendor46/Offer94541"),
		Bytes.toBytes("bsbm-inst_dataFromVendor52/Offer106582"),
		Bytes.toBytes("bsbm-inst_dataFromVendor59/Offer121179"),
		Bytes.toBytes("bsbm-inst_dataFromVendor64/Offer133949"),
		Bytes.toBytes("bsbm-inst_dataFromVendor71/Offer146132"),
		Bytes.toBytes("bsbm-inst_dataFromVendor78/Offer160176"),
		Bytes.toBytes("bsbm-inst_dataFromVendor86/Offer174377"),
		Bytes.toBytes("bsbm-inst_dataFromVendor92/Offer188178"),
			};

	public static byte[][] splitKeys100m = {
			// 100M BSBM triples
			Bytes.toBytes("bsbm-inst_dataFromProducer3812/Product192586"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite11/Review106912"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite156/Review1583657"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite208/Review2050480"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite258/Review2533161"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite46/Review454420"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite93/Review934878"),
			Bytes.toBytes("bsbm-inst_dataFromVendor1259/Offer2469756"),
			Bytes.toBytes("bsbm-inst_dataFromVendor1545/Offer3046577"),
			Bytes.toBytes("bsbm-inst_dataFromVendor1838/Offer3625153"),
			Bytes.toBytes("bsbm-inst_dataFromVendor2136/Offer4209225"),
			Bytes.toBytes("bsbm-inst_dataFromVendor2430/Offer4788228"),
			Bytes.toBytes("bsbm-inst_dataFromVendor2718/Offer5365352"),
			Bytes.toBytes("bsbm-inst_dataFromVendor411/Offer808411"),
			Bytes.toBytes("bsbm-inst_dataFromVendor703/Offer1384363") };

	public static byte[][] splitKeys1000m = {
			// 1000M
			Bytes.toBytes("bsbm-inst_dataFromProducer37580/Product1901928"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite1138/Review11410858"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite1503/Review15039554"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite2122/Review21273130"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite2797/Review28047687"),
			Bytes.toBytes("bsbm-inst_dataFromRatingSite809/Review8063849"),
			Bytes.toBytes("bsbm-inst_dataFromVendor11755/Offer23476170"),
			Bytes.toBytes("bsbm-inst_dataFromVendor14657/Offer29308042"),
			Bytes.toBytes("bsbm-inst_dataFromVendor17559/Offer35175767"),
			Bytes.toBytes("bsbm-inst_dataFromVendor20930/Offer41905838"),
			Bytes.toBytes("bsbm-inst_dataFromVendor23075/Offer46200350"),
			Bytes.toBytes("bsbm-inst_dataFromVendor25971/Offer52034910"),
			Bytes.toBytes("bsbm-inst_dataFromVendor28323/Offer56740397"),
			Bytes.toBytes("bsbm-inst_dataFromVendor5163/Offer10232563"),
			Bytes.toBytes("bsbm-inst_dataFromVendor7654/Offer15186319") };

}