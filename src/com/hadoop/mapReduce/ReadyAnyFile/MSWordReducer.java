package com.hadoop.mapReduce.ReadyAnyFile;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MSWordReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (LongWritable value : values) {
			sum += value.get();

		}
		context.write(key, new LongWritable(sum));
	}
}