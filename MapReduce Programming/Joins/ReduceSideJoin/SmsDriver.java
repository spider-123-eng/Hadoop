package com.hadoop.mapReduce.ReduceSideJoin;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmsDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.printf("Parameters are required- <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJobName("Map-side join with text lookup file in DCache");
		DistributedCache.addCacheFile(new URI(args[2]), conf);

		job.setJarByClass(SmsDriver.class);

		// specifying the custom reducer class
		job.setReducerClass(SmsReducer.class);

		job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
		job.setOutputValueClass(org.apache.hadoop.io.Text.class);
		
		// Specifying the input directories(@ runtime) and Mappers independently
		// for inputs from multiple sources
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, UserFileMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, DeliverFileMapper.class);

		// Specifying the output directory @ runtime
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new SmsDriver(),
				args);
		System.exit(exitCode);
	}
}