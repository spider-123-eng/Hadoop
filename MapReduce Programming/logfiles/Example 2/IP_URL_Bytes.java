package assignments;


import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Program to find the no of bytes consumed by each IP 


public class IP_URL_Bytes {
	
    public static final Pattern httplogPattern = Pattern
            .compile("([^\\s]+) - - \\[(.+)\\] \"([^\\s]+) (/[^\\s]*) HTTP/[^\\s]+\" [^\\s]+ ([0-9]+)");

    public static class AMapper extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Matcher matcher = httplogPattern.matcher(value.toString());
            if (matcher.matches()) {
            	String IP =  matcher.group(1);
            	String URL_Bytes =matcher.group(4) +" "+matcher.group(5);
                context.write(new Text(IP.trim()), new Text(URL_Bytes.trim()));
            }
        }
    }

    public static class AReducer extends Reducer<Text, Text, Text, Text> {
    	 private Text result = new Text();
    	 HashMap<String, String> map = new HashMap<String, String>();
        int sum = 0;
        StringBuilder requests = new StringBuilder();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
        	
        	String URL = ""; int bytesData;String duplicateURL = "";
        	for (Text val : values) {
        		
        		if(!map.containsKey(key.toString()))
        		{
        			map.put(key.toString(),val.toString());
        			String[] data = val.toString().split(" ");
            		URL = data[0];
            		String bytes = data[1];
            		bytesData = Integer.parseInt(bytes);
    			}
        		else
        		 {
        			String[] data = val.toString().split(" ");
        			URL = data[0];
        			String bytes = data[1];
        			bytesData = Integer.parseInt(bytes);
    			 }
        		
        	if(!URL.equalsIgnoreCase(duplicateURL)){
			 requests.append(URL).append("\t");
			 }
        	sum += bytesData;
            }
        	result.set("Total Size : "  +sum + "\t"+"  URl's  : " +requests);
            context.write(key, result);
            requests = new StringBuilder();
            sum = 0;
        }
    }
    
    
    
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(IP_URL_Bytes.class);
        job.setMapperClass(AMapper.class);
        job.setReducerClass(AReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
    
    
    
    
    


