package aws.emr.accesslog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class AccessLogReducerTest {

	private Reducer<Text, LongWritable, Text, LongWritable> reducer;

	private ReduceDriver<Text, LongWritable, Text, LongWritable> driver;

	@Before
	public void setUp() {
		reducer = new AccessLogReducer();
		driver = new ReduceDriver<Text, LongWritable, Text, LongWritable>(
				reducer);
	}

	@Test
	public void testWordCountReduce() throws IOException {
		// <foo : {1,1}>
		List<LongWritable> values = new ArrayList<LongWritable>();
		values.add(new LongWritable(1));
		driver.withInput(new Text("/test"), values);
		driver.withOutput(new Text("/test"), new LongWritable(1));
		List<LongWritable> values2 = new ArrayList<LongWritable>();
		values2.add(new LongWritable(1));
		values2.add(new LongWritable(1));
		driver.withInput(new Text("/foo/bar"), values2);
		driver.withOutput(new Text("/foo/bar"), new LongWritable(2));
		driver.runTest();
	}
}
