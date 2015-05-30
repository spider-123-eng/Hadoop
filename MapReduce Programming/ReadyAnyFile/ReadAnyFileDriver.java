package com.hadoop.mapReduce.ReadyAnyFile;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ReadAnyFileDriver  extends Configured implements Tool {
	
	public  String getFileExtension(File file) {
		  String fileName = file.getName();
        if(fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0)
        return fileName.substring(fileName.lastIndexOf(".")+1).trim();
        else return "";
    }
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("set the input path and output path");
			return 2;
		}
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(getClass());
		job.setJobName("Read any file");
		
		File file = new File(args[0]);
		String fileExt = getFileExtension(file);
		
		System.out.println("File Type  :" + fileExt);
		
		if(fileExt.equalsIgnoreCase("pdf")){
		
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
			job.setInputFormatClass(PdfInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);	
			
		}
		else if(fileExt.equalsIgnoreCase("xml")){

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setMapperClass(XMLCountMapper.class);
			job.setReducerClass(XMLCountReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
		}
		else if(fileExt.equalsIgnoreCase("txt")){

			job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(LongWritable.class);

	        job.setMapperClass(WordCountMapper.class);
	        job.setReducerClass(WordCountReducer.class);

	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        
		}else if(fileExt.equalsIgnoreCase("doc") || fileExt.equalsIgnoreCase("docx")){
			
			job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(LongWritable.class);

	        job.setMapperClass(WordCountMapper.class);
	        job.setReducerClass(WordCountReducer.class);
	        
	        job.setInputFormatClass(MSWordInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);

		}else if(fileExt.equalsIgnoreCase("xls") || fileExt.equalsIgnoreCase("xlsx")){
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
			job.setInputFormatClass(ExcelInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
		}else if(fileExt.equalsIgnoreCase("csv")){
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setMapperClass(BookXMapper.class);
			job.setReducerClass(BookXReducer.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
		
		}else{
			System.out.println("Unsuported file format found : "+file.getName());
			return 2;
		}
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReadAnyFileDriver(), args);
        System.exit(res);       
    }

}
