package com.hadoop.mapReduce.trendfinder;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author revanth
 */
public class TrendMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    
    // protected to allow unit testing
    public Text word = new Text();

    @Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {        	
        	String token = tokenizer.nextToken();
			if(token.startsWith("#")){
        		word.set(token.toLowerCase());
        		// Context here is like a multi set which allocates value "one" for key "word".
        		context.write(word, one);
        	}
        }
    }
}