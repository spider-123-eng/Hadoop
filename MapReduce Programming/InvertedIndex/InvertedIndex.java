package assignments;


import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InvertedIndex {

	
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
		public static final int RETAIlER_INDEX = 0;
		
		  public void map(LongWritable key, Text text, Context context) throws IOException,InterruptedException {
		  final String[] record = StringUtils.split(text.toString(), ",");
		  final String retailer = record[RETAIlER_INDEX];
		  for (int i = 1; i < record.length; i++) {
		   final String keyword = record[i];
		   context.write(new Text(keyword), new Text(retailer));
		   }
		  }
		 }
	
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		StringBuilder retailers = new StringBuilder();
		 public void reduce(Text text, Iterable<Text> values,Context context)throws IOException, InterruptedException {  
		  for (Text val : values) { 
			  retailers.append(val).append(",");
		  }
		  context.write(text, new Text(retailers.toString()));
		  retailers = new StringBuilder();
		  }
		 }
	 public static void main(String[] args) throws Exception {  
	       if (args.length != 2) {  
	        System.err.println("Usage: MaxTemperature <input path> <output path>");  
	        System.exit(-1);  
	       }  
	      
	       Job job = Job.getInstance(new Configuration());
	       job.setJarByClass(InvertedIndex.class);  
	       job.setJobName("max temperature");
	      
	       FileInputFormat.addInputPath(job, new Path(args[0]));  
	       FileOutputFormat.setOutputPath(job, new Path(args[1]));  
	      
	       job.setMapperClass(InvertedIndexMapper.class);  
	       job.setReducerClass(InvertedIndexReducer.class);  
	      
	       job.setOutputKeyClass(Text.class);  
	       job.setOutputValueClass(Text.class);  
	      
	       System.exit(job.waitForCompletion(true) ? 0 : 1);  
	      }  
}
