package com.hadoop.mapReduce.xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class XMLDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.println("set the input path and output path");
			return 2;
		}
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(getClass());
		job.setJobName("XML Reader");
		
		job.setInputFormatClass(TextInputFormat.class);
		
		
		job.setMapperClass(XMLCountMapper.class);
		job.setReducerClass(XMLCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new XMLDriver(), args);
        System.exit(res);       
    }

}
