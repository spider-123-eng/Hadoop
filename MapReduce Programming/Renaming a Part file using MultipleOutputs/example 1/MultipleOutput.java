package com.hadoop.mapReduce.MultipleOutputs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MultipleOutput {
    
    static String fruitOutputName = "fruit";
    static String colorOutputName = "color";

    public static class myMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");
            context.write(new Text(tokens[0]), value);
        }
    }

    public static class myReducer extends Reducer<Text, Text, Text, Writable> {
        MultipleOutputs<Text, Text> mos;

        public void setup(Context context) {
            mos = new MultipleOutputs(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String str = value.toString();
                String[] items = str.split("\t");
                
                mos.write(fruitOutputName, NullWritable.get(), new Text(items[1]), "fruit");
                mos.write(colorOutputName, NullWritable.get(), new Text(items[2]), "color");
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputDir = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        Configuration conf = new Configuration();

        Job job = new Job(conf);
        job.setJarByClass(MultipleOutput.class);
        job.setJobName("MultipleOutputs Test");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(myMapper.class);
        job.setReducerClass(myReducer.class);
        
        LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        MultipleOutputs.addNamedOutput(job, fruitOutputName, TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, colorOutputName, TextOutputFormat.class, NullWritable.class, Text.class);

        job.waitForCompletion(true);
    }
}

