package com.hadoop.mapReduce.xml;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class XMLCountReducer extends Reducer<Text, Text, Text, Text> {
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}

}
