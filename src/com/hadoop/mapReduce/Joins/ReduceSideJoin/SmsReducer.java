package com.hadoop.mapReduce.ReduceSideJoin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SmsReducer extends Reducer<Text, Text, Text, Text> {

	// Variables to aid the join process
	private String customerName, deliveryReport;
	private BufferedReader brReader;

	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}

	/*
	 * Map to store Delivery Codes and MessagesKey being the status code and
	 * vale being the status message
	 */
	private static HashMap<String, String> DeliveryCodesMap = new HashMap<String, String>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

		for (Path eachPath : cacheFilesLocal) {
			if (eachPath.getName().toString().trim()
					.equals("DeliveryStatusCodes.txt")) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadDeliveryStatusCodes(eachPath, context);
			}
		}

	}

	// To load the Delivery Codes and Messages into a hash map
	private void loadDeliveryStatusCodes(Path filePath, Context context)
			throws IOException {
		String strLineRead = "";
		try {
			brReader = new BufferedReader(new FileReader(filePath.toString()));

			// Read each line, split and load to HashMap
			while ((strLineRead = brReader.readLine()) != null) {
				String splitarray[] = strLineRead.split(",");
				DeliveryCodesMap
						.put(splitarray[0].trim(), splitarray[1].trim());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
		} catch (IOException e) {
			context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
			e.printStackTrace();
		} finally {
			if (brReader != null) {
				brReader.close();
			}
		}
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String currValue = value.toString();
			String valueSplitted[] = currValue.split("~");
			/*
			 * identifying the record source that corresponds to a cell number
			 * and parses the values accordingly
			 */
			if (valueSplitted[0].equals("CD")) {
				customerName = valueSplitted[1].trim();
			} else if (valueSplitted[0].equals("DR")) {
				// getting the delivery code and using the same to obtain the
				// Message
				deliveryReport = DeliveryCodesMap.get(valueSplitted[1].trim());
			}
		}
		// pump final output to file
		if (customerName != null && deliveryReport != null) {
			context.write(new Text(customerName), new Text(deliveryReport));
		} else if (customerName == null)
			context.write(new Text("customerName"), new Text(deliveryReport));
		else if (deliveryReport == null)
			context.write(new Text(customerName), new Text("deliveryReport"));
	}

}
