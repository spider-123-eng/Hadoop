import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountApp {
	public static class Map extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
		     StringTokenizer tokenizer = new StringTokenizer(line);
		     while (tokenizer.hasMoreTokens()) 
		     {
		      word.set(tokenizer.nextToken());
		      context.write(word, one);
		     }
		}

		public static class Reduce extends
				Reducer<Text, LongWritable, Text, LongWritable> {
			public void reduce(Text key, Iterable<LongWritable> values,
					Context context) throws IOException, InterruptedException {
				int sum = 0;
				for (LongWritable val : values) {
					sum += val.get();
				}
				context.write(key, new LongWritable(sum));
			}
		}

		public static void main(String[] args) throws Exception {
			
			Job job = Job.getInstance(new Configuration(),"WordCount Using MapReduce");
			job.setJarByClass(WordCountApp.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);
		}

	}
}