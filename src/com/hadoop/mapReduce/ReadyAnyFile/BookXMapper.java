package com.hadoop.mapReduce.ReadyAnyFile;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BookXMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String TempString = value.toString();
		String[] SingleBookData = TempString.split("\";\"");
		context.write(new Text(SingleBookData[3]), one);
	}
}
