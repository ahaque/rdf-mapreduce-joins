package tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.BSBMDataSetProcessor.Triple;

import java.io.IOException;
import java.util.List;

public class TransformNTtoKeys extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
			List<Triple> tripleList = BSBMDataSetProcessor.process(value.toString());
			for (Triple t : tripleList) {
				context.write(new Text(t.subject), one);
			}
		}

  }
	
	public static class Reduce1 extends Reducer<Text, IntWritable, Text, Text> {
		@SuppressWarnings({ "unchecked", "rawtypes" })
		protected void reduce(Text key, Iterable<IntWritable> value, Mapper.Context context) throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new TransformNTtoKeys(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {

    // Arguments: <input path> <output path>

    Job job = new Job(getConf());

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJobName("TransformNTtoKeys");
    job.setJarByClass(TransformNTtoKeys.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    return job.waitForCompletion(true) ? 0 : 1;

  }
}
