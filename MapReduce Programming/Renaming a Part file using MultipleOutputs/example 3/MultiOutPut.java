package com.hadoop.mapReduce.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;


public class MultiOutPut{
        static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
                public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
                        String [] dall=value.toString().split(":");
                        output.collect(new Text(dall[0]),new Text(dall[1]));
                }
        }

        static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
                public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
                        while (values.hasNext()) {
                                output.collect(key, values.next());
                        }
                }
        }


        static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
                protected String generateFileNameForKeyValue(Text key, Text value,String name) {
                        return key.toString();
                }
        }


        public static void main(String[] args) throws Exception {
                String InputFiles=args[0];
                String OutputDir=args[1];

                Configuration mycon=new Configuration();
                JobConf conf = new JobConf(mycon,MultiOutPut.class);

                conf.setOutputKeyClass(Text.class);
                conf.setMapOutputKeyClass(Text.class);
                conf.setOutputValueClass(Text.class);

                conf.setMapperClass(Map.class);
                conf.setReducerClass(Reduce.class);

                conf.setInputFormat(TextInputFormat.class);
                conf.setOutputFormat(MultiFileOutput.class);

                FileInputFormat.setInputPaths(conf,InputFiles);
                FileOutputFormat.setOutputPath(conf,new Path(OutputDir));
                JobClient.runJob(conf);

        }
}

