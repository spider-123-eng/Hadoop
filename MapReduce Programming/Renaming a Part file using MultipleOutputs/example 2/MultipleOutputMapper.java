package com.hadoop.mapReduce.MultipleOutputs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleOutputMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Text txtKey = new Text("");
	private Text txtValue = new Text("");

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (value.toString().length() > 0) {
			String[] custArray = value.toString().split(",");
			txtKey.set(custArray[0].toString());
			txtValue.set(custArray[1].toString() + "\t"
					+ custArray[3].toString());
			context.write(txtKey, txtValue);
		}
	}

}
