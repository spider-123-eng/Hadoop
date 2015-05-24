package com.hadoop.mapReduce.Custom.Partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
 
public class WordCountPartitioner extends Partitioner<Text, IntWritable>{
 
	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		if(numPartitions == 2){
			String partitionKey = key.toString();
			if(partitionKey.charAt(0) < 'a' )
				return 0;
			else 
				return 1;
		}else if(numPartitions == 1)
			return 0;
		else{
			System.err.println("WordCountParitioner can only handle either 1 or 2 paritions");
			return 0;
		}
	}
}

