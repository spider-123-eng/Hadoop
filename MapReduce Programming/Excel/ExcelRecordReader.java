package com.hadoop.mapReduce.Excel;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

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
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.hadoop.mapReduce.ReadyAnyFile.ReadAnyFileDriver;

public class ExcelRecordReader extends RecordReader<LongWritable, Text> {

	private String[] lines = null;
	private LongWritable key = null;
	private Text value = null;
	XSSFRow row;
	XSSFCell cell;
	String cell1 = null;
	String cell2 = null;
	String cell3 = null;
	
	
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		final Path file = split.getPath();
		//System.out.println("File Name   :" + split.getPath().getName());
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		ReadAnyFileDriver getExtn = new ReadAnyFileDriver();
		File fileName = new File(split.getPath().getName());
		
		if(getExtn.getFileExtension(fileName).equalsIgnoreCase("xls")){
			HSSFWorkbook wb = new HSSFWorkbook(fileIn);
			HSSFSheet sheet=wb.getSheetAt(0);
			HSSFRow row; 
			HSSFCell cell;
			Iterator rows = sheet.rowIterator();
	 
			while (rows.hasNext())
			{
				row=(HSSFRow) rows.next();
				Iterator cells = row.cellIterator();
				
				while (cells.hasNext())
				{
					cell=(HSSFCell) cells.next();
			
					if (cell.getCellType() == HSSFCell.CELL_TYPE_STRING)
					{
						cell1 += cell.getStringCellValue() + " ";
					}
					else if(cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC)
					{
						cell2 += Double.toString(cell.getNumericCellValue()).replace(".0", "") + " ";						System.out.print(cell.getNumericCellValue()+" ");
					}
					else
					{
						//U Can Handel Boolean, Formula, Errors
					}
				}
			}
		}
		else if(getExtn.getFileExtension(fileName).equalsIgnoreCase("xlsx")){
			
			XSSFWorkbook WorkBook = new XSSFWorkbook(fileIn);
			XSSFSheet sheet = WorkBook.getSheetAt(0);
			Iterator rows = sheet.rowIterator();
			
			while (rows.hasNext()) {
				row = (XSSFRow) rows.next();
				Iterator cells = row.cellIterator();
				while (cells.hasNext()) {
					cell = (XSSFCell) cells.next();

					if (cell.getCellType() == XSSFCell.CELL_TYPE_STRING) {
						// System.out.print(cell.getStringCellValue()+" ");
						cell1 += cell.getStringCellValue() + " ";
					} else if (cell.getCellType() == XSSFCell.CELL_TYPE_NUMERIC) {
						// System.out.print(cell.getNumericCellValue()+" ");

						cell2 += Double.toString(cell.getNumericCellValue()).replace(".0", "") + " ";
					} else {
						// U Can Handel Boolean, Formula, Errors
					}
				}

			}

			
		}
		
		cell3 = cell1.replaceAll("null", "") + " "+ cell2.replaceAll("null", "");
		this.lines = cell3.split(" ");
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
