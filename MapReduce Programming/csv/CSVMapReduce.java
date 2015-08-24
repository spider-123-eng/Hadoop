package com.hadoop.mapReduce.csv;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.mapReduce.pdf.PdfInputDriver;

public class CSVMapReduce extends Configured implements Tool {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] stringArray = value.toString().split(",");
			String line = value.toString().trim();

			String symbol = stringArray[0].toString().trim();
/*			String date = stringArray[1].toString().trim();
			double open = Double.parseDouble(stringArray[2].toString().trim());
			double high = Double.parseDouble(stringArray[3].toString().trim());
			double low = Double.parseDouble(stringArray[4].toString().trim());
			double close = Double.parseDouble(stringArray[5].toString().trim());
			int volume = Integer.parseInt(stringArray[6].toString().trim());
			int week = Integer.parseInt(stringArray[7].toString().trim());
			int weekday = Integer.parseInt(stringArray[8].toString().trim());
			String category = stringArray[10].toString().trim();*/
			String description = stringArray[9].toString().trim();
			
			context.write(new Text(symbol + " " + description), new Text(line));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Integer ctr = 0;
			Iterator<Text> it = values.iterator();

			while (it.hasNext()) {
				ctr++;
				it.next();
			}
			context.write(key, new Text(ctr.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CSVMapReduce(),
				args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out
					.println("Proper i/n and o/p directories should be given");
			System.exit(-1);
		}
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(CSVMapReduce.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
	}
}
