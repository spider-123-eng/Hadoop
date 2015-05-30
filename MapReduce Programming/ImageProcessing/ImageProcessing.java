

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImageProcessing extends Configured implements Tool{

	public static class ImageMapper extends Mapper<Text, BytesWritable, BytesWritable, Text> {


	    @Override
	    public void map(Text key, BytesWritable value, Context output) throws IOException,
	            InterruptedException {

	        output.write( value ,key );
	    }
	}
	
	public static class ImageReducer extends Reducer<BytesWritable, Text, Text, NullWritable> {

	    @Override
	    public void reduce(BytesWritable key, Iterable<Text> values, Context output)
	            throws IOException, InterruptedException {
	        Text imageFilePath = null;
	        for(Text filePath: values){
	        	imageFilePath = filePath;
	        	break;
	        }
	        output.write(imageFilePath,null);
	    }
	}
	
	 public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new ImageProcessing(), args);
	        System.exit(res);       
	    }

	    @Override
	    public int run(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.out.println("usage: [input] [output]");
	            System.exit(-1);
	        }

	        Job job = Job.getInstance(new Configuration());
	        job.setJarByClass(ImageProcessing.class);
	        job.setMapperClass(ImageMapper.class);
	        job.setReducerClass(ImageReducer.class);

	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setMapOutputKeyClass(BytesWritable.class);
	        job.setMapOutputValueClass(Text.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(NullWritable.class);

	        FileInputFormat.setInputPaths(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      

	        return job.waitForCompletion(true) ? 0 : 1;
	    }

	
}
