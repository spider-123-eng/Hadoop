package com.hadoop.mapReduce.Custom.DataType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WebLogDriver extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WebLogDriver(), args);
        System.exit(res);       
    }
	@Override
	  public int run(String[] args) throws Exception {
			if (args.length != 2) {
	          System.out.println("Proper i/n and o/p directories should be given");
	          System.exit(-1);
	      }
			
			Job job = Job.getInstance(new Configuration());
			job.setJobName("WebLog Reader");

		      job.setJarByClass(WebLogDriver.class);

		      job.setMapperClass(WebLogMapper.class);
		      job.setReducerClass(WebLogReducer.class);


		      job.setOutputKeyClass(Text.class);
		      job.setOutputValueClass(IntWritable.class);

		      job.setMapOutputKeyClass(WebLogWritable.class);
		      job.setMapOutputValueClass(IntWritable.class);

		      FileInputFormat.addInputPath(job, new Path(args[0]));
		      FileOutputFormat.setOutputPath(job, new Path(args[1]));

		      System.exit(job.waitForCompletion(true) ? 0 : 1);
			
			return 0;
	}
}
