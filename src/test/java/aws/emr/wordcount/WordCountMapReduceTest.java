package aws.emr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountMapReduceTest {

	private Mapper<LongWritable, Text, Text, IntWritable> mapper;
	private Reducer<Text, IntWritable, Text, IntWritable> reducer;

	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> driver;

	@Before
	public void setUp() {
		mapper = new WordCountMapper();
		reducer = new WordCountReducer();
		driver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>(
				mapper, reducer);
	}

	@Test
	public void testTokenizerMapper() throws IOException {
		driver.withInput(new LongWritable(1), new Text(
				"foo bar foobar foo bar"));
		driver.withInput(new LongWritable(1), new Text(
				"bar foo"));
		driver.withOutput(new Text("bar"), new IntWritable(3));
		driver.withOutput(new Text("foo"), new IntWritable(3));
		driver.withOutput(new Text("foobar"), new IntWritable(1));
		driver.runTest();
	}
}
