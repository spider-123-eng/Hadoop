package com.hadoop.mapReduce.tika;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

public class TikaRecordReader extends RecordReader<Text, Text> {
	private Text key = new Text();
	private Text value = new Text();
	private FileSplit fileSplit;
	private Configuration conf;
	private boolean processed = false;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (!processed) {
			Path path = fileSplit.getPath();
			key.set(path.toString());
			@SuppressWarnings("unused")
			FileSystem fs = path.getFileSystem(conf);
			@SuppressWarnings("unused")
			FSDataInputStream fin = null;

			try {
				String con = new Tika().parseToString(new URL(path.toString()));
				String string = con.replaceAll("[$%&+,:;=?#|']", " ");
				String string2 = string.replaceAll("\\s+", " ");
				String lo = string2.toLowerCase();
				value.set(lo);
			} catch (TikaException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			processed = true;
			return true;
		} else {
			return false;

		}

	}

}
