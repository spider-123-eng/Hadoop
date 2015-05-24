package com.hadoop.mapReduce.Custom.Partitioner;

import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;
 
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
 
	org.slf4j.Logger logger = LoggerFactory.getLogger(WordCountMapper.class);
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		logger.debug("Entering WordCountMapper.map() " + this);
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line," ");
		while(st.hasMoreTokens()){
			word.set(st.nextToken());
			context.write(word,one);
		}
		
		logger.debug("Exiting WordCountMapper.map()");
	}
 
}
