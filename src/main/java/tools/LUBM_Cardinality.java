package main.java.tools;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Takes an input .nt triple file and counts the number of each data type
 * Counts total number of universities, undergrads, professors, etc.
 * 
 * MAPREDUCE STAGE 1:
 * Get unique subjects and their type
 * 
 * MAPREDUCE STAGE 2:
 * Count total types
 * 
 * @author Albert Haque
 * @date May 2014
 */

public class LUBM_Cardinality {
	
	private static final String INTERMEDIATE_FOLDER = "/temp/lubm_cardinality";
	
	public static void main(String[] args) throws Exception {
		String USAGE_MSG = "Arguments: <input nt file> <output location>";
		if (args == null || args.length != 2) {
			System.out.println("\n  You entered " + args.length + " arguments.");
			System.out.println("  " + USAGE_MSG);
			System.exit(0);
		}
	    
	    startJob_Stage1(args);
	    startJob_Stage2(args);
	 }

	/**
	 * MAPREDUCE STAGE 1: MAP
	 * Get subjects and their type
	 */
	public static class Stage1_Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] triple = value.toString().split(" ");
			if (triple[1].equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")) {
				context.write(new Text(triple[0]), new Text(triple[2]));
			}
		}
	}

	/**
	 * MAPREDUCE STAGE 1: REDUCE
	 * Get UNIQUE subjects
	 */
	public static class Stage1_Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<Text> completedKeys = new HashSet<Text>();
			for (Text val : values) {
				if (!completedKeys.contains(val)) {
					context.write(key, val);
					completedKeys.add(val);
				}
			}
		}
	}
	
	/**
	 * MAPREDUCE STAGE 2: MAP
	 * Emit the type
	 */
	public static class Stage2_Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s+");
			context.write(new Text(line[1]), one);
		}
	}

	/**
	 * MAPREDUCE STAGE 2: REDUCE
	 * Count the types
	 */
	public static class Stage2_Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	 
		public static Job startJob_Stage1(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration conf = new Configuration();
	        
		    @SuppressWarnings("deprecation")
			Job job = new Job(conf, "LUBM_Cardinality-Stage1");
		    job.setJarByClass(LUBM_Cardinality.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    job.setMapperClass(Stage1_Map.class);
		    job.setReducerClass(Stage1_Reduce.class);
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    job.setNumReduceTasks(1);
		        
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE_FOLDER));
		    
		    try {
				job.waitForCompletion(true);
			} catch (ClassNotFoundException e) { e.printStackTrace(); }
			  catch (InterruptedException e) { e.printStackTrace();}

			return job;
		 }
		
		public static Job startJob_Stage2(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration conf = new Configuration();
	        
		    @SuppressWarnings("deprecation")
			Job job = new Job(conf, "LUBM_Cardinality-Stage2");
		    job.setJarByClass(LUBM_Cardinality.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setMapperClass(Stage2_Map.class);
		    job.setReducerClass(Stage2_Reduce.class);
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    job.setNumReduceTasks(1);
		        
		    String intermediateData = INTERMEDIATE_FOLDER + "/part-r-00000";
		    FileInputFormat.addInputPath(job, new Path(intermediateData));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		    try {
				job.waitForCompletion(true);
			} catch (ClassNotFoundException e) { e.printStackTrace(); }
			  catch (InterruptedException e) { e.printStackTrace();}
		    
		    // Delete the intermediate data
		    FileSystem fileSystem = FileSystem.get(conf);
		    fileSystem.delete(new Path(INTERMEDIATE_FOLDER), true);
		    

			return job;
		 }
}
