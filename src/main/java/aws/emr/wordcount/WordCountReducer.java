package aws.emr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		System.out.println(key);
		for (IntWritable value : values) {
			System.out.println(" " + value);
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
}