package com.hadoop.mapReduce.ReadyAnyFile;

import java.io.File;
import java.io.IOException;
import java.util.List;

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
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;

public class MSWordRecordReader extends RecordReader<LongWritable, Text> {

	private String[] lines = null;
	private LongWritable key = null;
	private Text value = null;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		String words = null;
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		final Path file = split.getPath();
		System.out.println("File Name   :"+split.getPath().getName());
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		ReadAnyFileDriver getExtn = new ReadAnyFileDriver();
		File fileName = new File(split.getPath().getName());
				
		try{
			if(getExtn.getFileExtension(fileName).equalsIgnoreCase("docx")){
				
				XWPFDocument document = new XWPFDocument(fileIn);
				 
				List<XWPFParagraph> paragraphs = document.getParagraphs();
				//System.out.println("Total no of paragraph "+paragraphs.size());
				
				for (XWPFParagraph para : paragraphs) {
					words = para.getText();
					this.lines = words.split(" ");
					//System.out.println(para.getText());
				}
			}
			else if(getExtn.getFileExtension(fileName).equalsIgnoreCase("doc")){
			
				HWPFDocument doc = new HWPFDocument(fileIn);
				 
				WordExtractor we = new WordExtractor(doc);
	 
				String[] paragraphs = we.getParagraphText();
				
				for (String para : paragraphs) {
					words = para.toString();
					this.lines = words.split(" ");
				}
				
			}
		
		}
		catch (Exception e) {
			e.printStackTrace();
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

