package com.hadoop.mapReduce.tika;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TikaOutPutFormt extends FileOutputFormat<Text, Text> {

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Path path=FileOutputFormat.getOutputPath(context);
		Path fullapth=new Path(path,"Srini.txt");
		FileSystem fs=path.getFileSystem(context.getConfiguration());
		FSDataOutputStream output=fs.create(fullapth,context);
		return new TikaRecordWrite(output);
	}

}
