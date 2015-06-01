import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {

		StringBuilder stringBuilder = new StringBuilder();

		for (Text value : values) {
			stringBuilder.append(value.toString());

			if (values.iterator().hasNext()) {
				stringBuilder.append(" | ");
			}
		}
		context.write(key, new Text(stringBuilder.toString()));
	}
}

/*If you want to add a delimited(Ex: =>) between key and value in reducer output, you can set the below property in driver class:
job.getConfiguration().set(“mapreduce.output.textoutputformat.separator”, ” => “);
Output looks like below then:
Desired reducer output: Word1 => Doc1 | Doc2 */