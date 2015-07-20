package com.hadoop.mapReduce.Custom.Partitioner;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionerDemo extends Configured implements Tool {
	/**
	 * Mapper class generating key value pair of game,country as intermediate keys
	 */
	public static class PartitionerMap extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");
			try{
			context.write(new Text(words[2]),new Text(words[1]));
			}
			catch(Exception e){
				System.err.println(e);
			}
		}
	}
	/**
	 * Each partition processed by different reducer tasks as defined in our custom partitioner
	 */
	public static class PartitionerReduce extends Reducer<Text,Text,Text,IntWritable> {
		public void reduce(Text key,Iterable<Text>  values,Context context) throws IOException, InterruptedException {
			int gameCount=0;
			for(Text val:values){
				gameCount++;
			}
			context.write(new Text(key),new IntWritable(gameCount));
		}
	}
	/**
	 * Our custom Partitioner class will divide the dataset into three partitions one with key as cricket and value as
	 * india, second partition with key as cricket and value other than india, and third partition with game(key) other 
	 * than cricket 
	 */
	public static class customPartitioner extends Partitioner<Text,Text>{
		public int getPartition(Text key, Text value, int numReduceTasks){
		if(numReduceTasks==0)
			return 0;
		if(key.equals(new Text("Cricket")))
			return 0;
		if(key.equals(new Text("Football")))
			return 1;
		else
			return 2;
		}
	}
	public static void main(String[] args) throws Exception {
		int res= ToolRunner.run(new Configuration(),new PartitionerDemo(),args);
		System.exit(res);
	}
	public int run(String[] args) throws Exception {
		if(args.length!=2){
			System.out.print("Run as --> hadoop jar /path/to/name.jar /inputdataset /output");
			System.exit(-1);
		}
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(PartitionerDemo.class);
		
		//Set number of reducer tasks
		job.setNumReduceTasks(3);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(PartitionerMap.class);
		job.setReducerClass(PartitionerReduce.class);

		//Set Partitioner Class
		job.setPartitionerClass(customPartitioner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}
