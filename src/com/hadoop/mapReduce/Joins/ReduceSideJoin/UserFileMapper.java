package com.hadoop.mapReduce.ReduceSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
  
 
public class UserFileMapper extends Mapper  <LongWritable, Text, Text, Text>
{    
    //variables to process Consumer Details
       private String cellNumber,customerName,fileTag="CD~";
      
       /* map method that process ConsumerDetails.txt and frames the initial key value pairs
        * Key(Text) – mobile number
        * Value(Text) – An identifier to indicate the source of input(using ‘CD’ for the customer details file) + Customer Name
        * */  
      
       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          //taking one line/record at a time and parsing them into key value pairs
          String line = value.toString();
          String splitarray[] = line.split(",");
          cellNumber = splitarray[0].trim();
          customerName = splitarray[1].trim();
           
         //sending the key value pair out of mapper
          context.write(new Text(cellNumber), new Text(fileTag+customerName));
          }     
       }
