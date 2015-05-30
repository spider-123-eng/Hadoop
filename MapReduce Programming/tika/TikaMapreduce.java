package com.hadoop.mapReduce.tika;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TikaMapreduce extends Configured implements Tool {
	public static class TikaMapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new Configuration(), new TikaMapreduce(),
				args);
		System.exit(exit);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.println("set the input path and output path");
			return 2;
		}
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(getClass());
		job.setJobName("TikRead");
		job.setInputFormatClass(TikaFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(TikaMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TikaOutPutFormt.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]
				+ System.currentTimeMillis()));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
