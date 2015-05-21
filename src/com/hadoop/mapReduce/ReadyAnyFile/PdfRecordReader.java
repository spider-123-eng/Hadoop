package com.hadoop.mapReduce.ReadyAnyFile;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

public class PdfRecordReader extends RecordReader<LongWritable, Text> {

	private String[] lines = null;
	private LongWritable key = null;
	private Text value = null;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		PDDocument pdf = null;
		String parsedText = null;
		try{
		PDFTextStripper pdfStripper= new PDFTextStripper();
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		final Path file = split.getPath();
		System.out.println("File Name   :"+split.getPath().getName());
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		pdf = PDDocument.load(fileIn);
		pdfStripper.setStartPage(1);
		pdfStripper.setEndPage(5);
		parsedText = pdfStripper.getText(pdf);
		String string = parsedText.replaceAll("([$%&+,:;=?#|'])", " ");
		String string2 = string.replaceAll("\\s+", " ");
		/*String parsedTextSplit[] = string2.split(" ");
		for(int i=0;i<parsedTextSplit.length;i++){
			
			System.out.println("parsedText "+parsedTextSplit[i]);
			
		}*/
		this.lines = string2.split(" ");
		}finally{}
		if (pdf != null) {
			pdf.close();
		}
		}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (key == null) {
			key = new LongWritable();
			key.set(1);
			value = new Text();
			value.set(lines[0]);
		} else {
			int temp = (int) key.get();
			if (temp < (lines.length - 1)) {
				int count = (int) key.get();
				value = new Text();
				value.set(lines[count]);
				count = count + 1;
				key = new LongWritable(count);
			} else {
				return false;
			}

		}
		if (key == null || value == null) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {

		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {

		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return 0;
	}

	@Override
	public void close() throws IOException {

	}

}
