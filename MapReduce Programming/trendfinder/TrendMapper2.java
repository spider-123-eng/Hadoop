package com.hadoop.mapReduce.trendfinder;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author revanth
 */
public class TrendMapper2 extends Mapper<LongWritable, Text, LongWritable, Text>{
	

    @Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
        String line = value.toString(); // agilencr 4
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
        	
        	String token = tokenizer.nextToken();	
        
        // Context here is like a multi set which allocates value "one" for key "word".
        	
        	context.write(new LongWritable(Long.parseLong(tokenizer.nextToken().toString())), new Text(token));      	
        	
        }
    }

}
