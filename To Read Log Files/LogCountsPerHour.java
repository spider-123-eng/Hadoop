package com.mapReduce.logFiles;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mapReduce.PDFCount.PdfInputDriver;

public class LogCountsPerHour extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LogCountsPerHour(),args);
		System.exit(res);
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}
		
	 	
		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(DateWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(LogMapClass.class);
		job.setReducerClass(LogReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(PdfInputDriver.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("Job Finished!");
        return 0;
	}

	public static class LogMapClass extends
			Mapper<LongWritable, Text, DateWritable, IntWritable> {
		private DateWritable date = new DateWritable();
		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// Get the value as a String; it is of the format:
			// 111.111.111.111 - - [16/Dec/2012:05:32:50 -0500] "GET / HTTP/1.1"
			// 200 14791 "-"
			// "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)"
			String text = value.toString();

			// Get the date and time
			int openBracket = text.indexOf('[');
			int closeBracket = text.indexOf(']');
			if (openBracket != -1 && closeBracket != -1) {
				// Read the date
				String dateString = text.substring(text.indexOf('[') + 1,
						text.indexOf(']'));

				// Build a date object from a string of the form:
				// 16/Dec/2012:05:32:50 -0500
				int index = 0;
				int nextIndex = dateString.indexOf('/');
				int day = Integer.parseInt(dateString.substring(index,
						nextIndex));

				index = nextIndex;
				nextIndex = dateString.indexOf('/', index + 1);
				String month = dateString.substring(index + 1, nextIndex);

				index = nextIndex;
				nextIndex = dateString.indexOf(':', index);
				int year = Integer.parseInt(dateString.substring(index + 1,
						nextIndex));

				index = nextIndex;
				nextIndex = dateString.indexOf(':', index + 1);
				int hour = Integer.parseInt(dateString.substring(index + 1,
						nextIndex));

				// Build a calendar object for this date
				Calendar calendar = Calendar.getInstance();
				calendar.set(Calendar.DATE, day);
				calendar.set(Calendar.YEAR, year);
				calendar.set(Calendar.HOUR, hour);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);

				if (month.equalsIgnoreCase("dec")) {
					calendar.set(Calendar.MONTH, Calendar.DECEMBER);
				} else if (month.equalsIgnoreCase("nov")) {
					calendar.set(Calendar.MONTH, Calendar.NOVEMBER);
				} else if (month.equalsIgnoreCase("oct")) {
					calendar.set(Calendar.MONTH, Calendar.OCTOBER);
				} else if (month.equalsIgnoreCase("sep")) {
					calendar.set(Calendar.MONTH, Calendar.SEPTEMBER);
				} else if (month.equalsIgnoreCase("aug")) {
					calendar.set(Calendar.MONTH, Calendar.AUGUST);
				} else if (month.equalsIgnoreCase("jul")) {
					calendar.set(Calendar.MONTH, Calendar.JULY);
				} else if (month.equalsIgnoreCase("jun")) {
					calendar.set(Calendar.MONTH, Calendar.JUNE);
				} else if (month.equalsIgnoreCase("may")) {
					calendar.set(Calendar.MONTH, Calendar.MAY);
				} else if (month.equalsIgnoreCase("apr")) {
					calendar.set(Calendar.MONTH, Calendar.APRIL);
				} else if (month.equalsIgnoreCase("mar")) {
					calendar.set(Calendar.MONTH, Calendar.MARCH);
				} else if (month.equalsIgnoreCase("feb")) {
					calendar.set(Calendar.MONTH, Calendar.FEBRUARY);
				} else if (month.equalsIgnoreCase("jan")) {
					calendar.set(Calendar.MONTH, Calendar.JANUARY);
				}

				// Output the date as the key and 1 as the value
				date.setDate(calendar.getTime());
				context.write(date, one);

			}
		}
	}

	public static class LogReduce extends
			Reducer<DateWritable, IntWritable, DateWritable, IntWritable> {
		public void reduce(DateWritable key, Iterable<IntWritable> values,
				Context output) throws IOException, InterruptedException {
			// Iterate over all of the values (counts of occurrences of this
			// word)
		
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();

			}
			// Output the word with its count (wrapped in an IntWritable)
			output.write(key, new IntWritable(sum));
		}
	}

}
