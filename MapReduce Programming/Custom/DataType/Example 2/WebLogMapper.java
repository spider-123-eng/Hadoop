package com.hadoop.mapReduce.Custom.DataType;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebLogMapper extends Mapper <LongWritable, Text, WebLogWritable, IntWritable>
{
  private static final IntWritable one = new IntWritable(1);
  
  private WebLogWritable wLog = new WebLogWritable();

  private IntWritable reqno = new IntWritable();
  private Text url = new Text();
  private Text rdate = new Text();
  private Text rtime = new Text();
  private Text rip = new Text();

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
    String[] words = value.toString().split("\t") ;

    reqno.set(Integer.parseInt(words[0]));
    url.set(words[1]);
    rdate.set(words[2]);
    rtime.set(words[3]);
    rip.set(words[4]);

    wLog.set(reqno, url, rdate, rtime, rip);

    context.write(wLog, one);
  }
}
