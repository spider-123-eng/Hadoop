package com.hadoop.mapReduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VotesCountApp extends Configured implements Tool{

	public static class VoteCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	    private final static IntWritable one = new IntWritable(1);

	    @Override
	    public void map(Object key, Text value, Context output) throws IOException,
	            InterruptedException {

	        //If more than one word is present, split using white space.
	        String[] words = value.toString().split(" ");
	        //Only the first word is the candidate name
	        output.write(new Text(words[0]), one);
	    }
	}
	
	public static class VoteCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	    @Override
	    public void reduce(Text key, Iterable<IntWritable> values, Context output)
	            throws IOException, InterruptedException {
	        int voteCount = 0;
	        for(IntWritable value: values){
	            voteCount+= value.get();
	        }
	        output.write(key, new IntWritable(voteCount));
	    }
	}
	
	 public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new VotesCountApp(), args);
	        System.exit(res);       
	    }

	    @Override
	    public int run(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.out.println("usage: [input] [output]");
	            System.exit(-1);
	        }

	        Job job = Job.getInstance(new Configuration());
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);

	        job.setMapperClass(VoteCountMapper.class);
	        job.setReducerClass(VoteCountReducer.class);

	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);

	        FileInputFormat.setInputPaths(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        job.setJarByClass(VotesCountApp.class);

	        job.submit();
	        return 0;
	    }

	
}
