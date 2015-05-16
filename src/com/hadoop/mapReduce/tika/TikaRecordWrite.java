package com.hadoop.mapReduce.tika;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TikaRecordWrite extends RecordWriter<Text, Text> {
         private DataOutputStream out;
	public TikaRecordWrite(DataOutputStream output) {
		// TODO Auto-generated constructor stub
		out=output;
		try {
			out.writeBytes("result:\r\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
             out.close();
	}

	@Override
	public void write(Text key, Text value) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
             out.writeBytes(key.toString());
             out.writeBytes(",");
             out.writeBytes(value.toString());
             out.writeBytes("\r\n"); 
	}

}
