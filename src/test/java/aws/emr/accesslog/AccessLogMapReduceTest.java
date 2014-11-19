package aws.emr.accesslog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class AccessLogMapReduceTest {

	private Mapper<LongWritable, Text, Text, LongWritable> mapper;
	private Reducer<Text, LongWritable, Text, LongWritable> reducer;

	private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> driver;

	@Before
	public void setUp() {
		mapper = new AccessLogMapper();
		reducer = new AccessLogReducer();
		driver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>(
				mapper, reducer);
	}

	@Test
	public void testAccessLogMapReduce() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(
				AccessLogMapperTest.class
						.getResourceAsStream("/aws/emr/accesslog/access.log")));

		String line = null;
		while ((line = br.readLine()) != null) {
			driver.withInput(new LongWritable(1), new Text(line));
		}

		driver.withOutput(new Text("/"), new LongWritable(1));
		driver.withOutput(new Text("/foo/bar"), new LongWritable(1));
		driver.withOutput(new Text("/test"), new LongWritable(2));
		driver.runTest();
	}
}
