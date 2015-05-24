package com.hadoop.mapReduce.Custom.Partitioner;

import java.util.Date;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
 
public class WordCountDriver extends Configured implements Tool{
 
    Logger logger = LoggerFactory.getLogger(WordCountDriver.class);
 
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new WordCountDriver(), args);
        System.exit(exitCode);
 
    }
 
    public int run(String[] args) throws Exception {
    	Date startTime = new Date();
    	   System.out.println("Start time: " + startTime.toString());
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
 
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCountDriver.class);
        job.setJobName("WordCounter");
        logger.info("Input path " + args[0]);
        logger.info("Oupput path " + args[1]);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setNumReduceTasks(2);
        job.setPartitionerClass(WordCountPartitioner.class);
        
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
 
        int returnValue = job.waitForCompletion(true) ? 0:1;
        System.out.println("job.isSuccessful " + job.isSuccessful());
        
        Date endTime = new Date();
        System.out.println("Process Completed: " + endTime);
        long timeTakenInSec = endTime.getTime() - startTime.getTime();
        System.out.println("Time taken: " + (timeTakenInSec / 1000) + " secs " + (timeTakenInSec % 1000) + " ms");
        return returnValue;
    }
 
}

