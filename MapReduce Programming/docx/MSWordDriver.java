package com.hadoop.mapreduce.docx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.mapReduce.trendfinder.ReverseComparator;

public class MSWordDriver extends Configured implements Tool {
	 public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new MSWordDriver(), args);
	        System.exit(res);       
	    }

	    @Override
	    public int run(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.out.println("usage: [input] [output]");
	            System.exit(-1);
	        }

	        Job job = Job.getInstance(new Configuration());
	        job.setJarByClass(MSWordDriver.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(LongWritable.class);

	        job.setMapperClass(MSWordMapper.class);
	        job.setReducerClass(MSWordReducer.class);
	        
	        // To print in the reverse order .
	        job.setSortComparatorClass(ReverseComparator.class);
	        job.setInputFormatClass(MSWordInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);

	        FileInputFormat.setInputPaths(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	        System.out.println("Job Finished!");
	        return 0;
	    }

}
