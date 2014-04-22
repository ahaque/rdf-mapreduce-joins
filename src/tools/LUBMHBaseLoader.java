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
public class LUBMHBaseLoader extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	
	// Used for Split-Key Generation at the bottom of this file
	public static byte[][] splitKeys = {};

  private long timestamp = 0;


  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    for (LUBMDataSetProcessor.Triple triple : LUBMDataSetProcessor.process(value.toString())) {

      byte[] subject = toBytes(triple.subject);

      Put put = new Put(subject);

      // if the triple is not a valu triple we store the object as the col name and the predicate as the col
      // value so that we can do bloom filtered s-o joins
      if (!triple.isValueTriple()) {
        put.add(toBytes(LUBMDataSetProcessor.COLUMN_FAMILY),
          toBytes(triple.object),
          timestamp++,
          toBytes(triple.predicate));
        // if it is a value (like an int or a data) we wouldn't join on it anyway so store as usual
      } else {
        put.add(toBytes(LUBMDataSetProcessor.COLUMN_FAMILY),
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
        HColumnDescriptor colDesc = new HColumnDescriptor(LUBMDataSetProcessor.COLUMN_FAMILY);
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
    job.setJobName("LUBMToHBaseLoader");
    job.setJarByClass(LUBMHBaseLoader.class);
    job.setMapperClass(LUBMHBaseLoader.class);

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
		Bytes.toBytes("Department0.University32.edu/AssistantProfessor6/Publication6"),
		Bytes.toBytes("Department0.University55.edu/FullProfessor8/Publication0"),
		Bytes.toBytes("Department0.University77.edu/UndergraduateStudent32"),
		Bytes.toBytes("Department1.University27.edu/GraduateCourse10"),
		Bytes.toBytes("Department1.University5.edu/AssociateProfessor4/Publication1"),
		Bytes.toBytes("Department1.University71.edu/FullProfessor4/Publication7"),
		Bytes.toBytes("Department10.University21.edu/FullProfessor5/Publication15"),
		Bytes.toBytes("Department10.University45.edu/GraduateStudent96"),
		Bytes.toBytes("Department10.University68.edu/FullProfessor3/Publication4"),
		Bytes.toBytes("Department11.University18.edu/AssistantProfessor2/Publication7"),
		Bytes.toBytes("Department11.University40.edu/AssociateProfessor7/Publication0"),
		Bytes.toBytes("Department11.University62.edu/UndergraduateStudent265"),
		Bytes.toBytes("Department12.University12.edu/UndergraduateStudent118"),
		Bytes.toBytes("Department12.University35.edu/UndergraduateStudent358"),
		Bytes.toBytes("Department12.University59.edu/AssociateProfessor12/Publication4"),
		Bytes.toBytes("Department12.University9.edu/UndergraduateStudent81"),
		Bytes.toBytes("Department13.University30.edu/UndergraduateStudent10"),
		Bytes.toBytes("Department13.University52.edu/FullProfessor3/Publication9"),
		Bytes.toBytes("Department13.University75.edu/UndergraduateStudent96"),
		Bytes.toBytes("Department14.University26.edu/UndergraduateStudent248"),
		Bytes.toBytes("Department14.University49.edu/Course11"),
		Bytes.toBytes("Department14.University71.edu/UndergraduateStudent231"),
		Bytes.toBytes("Department15.University23.edu/UndergraduateStudent312"),
		Bytes.toBytes("Department15.University47.edu/FullProfessor3/Publication0"),
		Bytes.toBytes("Department15.University73.edu/AssociateProfessor12/Publication10"),
		Bytes.toBytes("Department16.University29.edu/GraduateStudent90"),
		Bytes.toBytes("Department16.University55.edu/UndergraduateStudent205"),
		Bytes.toBytes("Department17.University10.edu/GraduateStudent106"),
		Bytes.toBytes("Department17.University4.edu/UndergraduateStudent371"),
		Bytes.toBytes("Department17.University70.edu/UndergraduateStudent0"),
		Bytes.toBytes("Department18.University30.edu/AssistantProfessor2"),
		Bytes.toBytes("Department18.University65.edu/AssociateProfessor1/Publication14"),
		Bytes.toBytes("Department19.University33.edu/AssociateProfessor5/Publication15"),
		Bytes.toBytes("Department19.University71.edu/UndergraduateStudent278"),
		Bytes.toBytes("Department2.University26.edu/GraduateStudent55"),
		Bytes.toBytes("Department2.University50.edu/AssociateProfessor10/Publication6"),
		Bytes.toBytes("Department2.University73.edu/AssistantProfessor3"),
		Bytes.toBytes("Department20.University4.edu/UndergraduateStudent200"),
		Bytes.toBytes("Department21.University17.edu/FullProfessor7/Publication1"),
		Bytes.toBytes("Department22.University11.edu/UndergraduateStudent108"),
		Bytes.toBytes("Department23.University31.edu/GraduateStudent93"),
		Bytes.toBytes("Department3.University16.edu/AssociateProfessor4/Publication9"),
		Bytes.toBytes("Department3.University39.edu/Course34"),
		Bytes.toBytes("Department3.University61.edu/UndergraduateStudent248"),
		Bytes.toBytes("Department4.University11.edu/UndergraduateStudent129"),
		Bytes.toBytes("Department4.University35.edu/FullProfessor9/Publication14"),
		Bytes.toBytes("Department4.University58.edu/AssociateProfessor7/Publication4"),
		Bytes.toBytes("Department4.University9.edu/FullProfessor4/Publication3"),
		Bytes.toBytes("Department5.University30.edu/FullProfessor4/Publication1"),
		Bytes.toBytes("Department5.University53.edu/UndergraduateStudent335"),
		Bytes.toBytes("Department5. University77.edu/Course35"),
		Bytes.toBytes("Department6.University26.edu/UndergraduateStudent15"),
		Bytes.toBytes("Department6.University49.edu/UndergraduateStudent213"),
		Bytes.toBytes("Department6.University71.edu/FullProfessor5/Publication0"),
		Bytes.toBytes("Department7.University21.edu/GraduateStudent86"),
		Bytes.toBytes("Department7.University45.edu/AssociateProfessor4/Publication9"),
		Bytes.toBytes("Department7.University67.edu/UndergraduateStudent3"),
		Bytes.toBytes("Department8.University17.edu/UndergraduateStudent187"),
		Bytes.toBytes("Department8.University4.edu/AssociateProfessor9/Publication12"),
		Bytes.toBytes("Department8.University63.edu/AssistantProfessor5/Publication3"),
		Bytes.toBytes("Department9.University12.edu/UndergraduateStudent157"),
		Bytes.toBytes("Department9.University35.edu/GraduateStudent12"),
		Bytes.toBytes("Department9.University59.edu/AssociateProfessor3/Publication4"),
			};

	public static byte[][] splitKeys100m = {
			// 100M LUBM triples

			};

	public static byte[][] splitKeys1000m = {
			// 1000M

			};

}