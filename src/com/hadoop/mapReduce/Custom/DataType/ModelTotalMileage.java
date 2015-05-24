package com.hadoop.mapReduce.Custom.DataType;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ModelTotalMileage extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ModelTotalMileage(), args);
        System.exit(res);       
    }
  static final Logger logger = Logger.getLogger(ModelTotalMileage.class);

  public static class ModelMileageMapper extends Mapper<Object, Text, Text, Vehicle>{

    private Vehicle vehicle = new Vehicle();
    private Text model = new Text();

    public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

      String var[] = new String[6];
      var = value.toString().split(" "); 

      if(var.length == 3){
        model.set(var[0]);
        vehicle.setModel(var[0]);
        vehicle.setVin(var[1]);
        vehicle.setMileage(Integer.parseInt(var[2]));
        context.write(model, vehicle);
      }
    }
  }

  public static class ModelTotalMileageReducer extends Reducer<Text,Vehicle,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Vehicle> values,
        Context context
        ) throws IOException, InterruptedException {
      int totalMileage = 0;
      for (Vehicle vehicle : values) {
        totalMileage += vehicle.getMileage();
      }
      result.set(totalMileage);
      context.write(key, result);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
		if (args.length != 2) {
          System.out.println("Proper i/n and o/p directories should be given");
          System.exit(-1);
      }
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(ModelTotalMileage.class);
    
    job.setMapperClass(ModelMileageMapper.class);
    job.setReducerClass(ModelTotalMileageReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Vehicle.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    return 0;
  }

}

