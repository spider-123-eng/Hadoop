package com.hadoop.mapReduce.trendfinder;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author revanth
 */
public class TrendReducer2 extends Reducer<LongWritable, Text, Text, Text> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> trends, Context context)
			throws IOException, InterruptedException {

		 for (Text val : trends) {
			 context.write(new Text(val.toString()),new Text(key.toString()));
	        }
	}
}