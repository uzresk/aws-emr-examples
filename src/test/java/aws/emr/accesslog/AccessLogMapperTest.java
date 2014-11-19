package aws.emr.accesslog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class AccessLogMapperTest extends TestCase {

	private Mapper<LongWritable, Text, Text, LongWritable> mapper;

	private MapDriver<LongWritable, Text, Text, LongWritable> driver;

	@Before
	public void setUp() {

		mapper = new AccessLogMapper();
		driver = new MapDriver<LongWritable, Text, Text, LongWritable>(mapper);
	}

	@Test
	public void testCount() throws IOException {

		BufferedReader br = new BufferedReader(new InputStreamReader(
				AccessLogMapperTest.class
						.getResourceAsStream("/aws/emr/accesslog/access.log")));

		String line = null;
		while ((line = br.readLine()) != null) {
			driver.withInput(new LongWritable(1), new Text(line));
		}

		driver.withOutput(new Text("/"), new LongWritable(1));
		driver.withOutput(new Text("/test"), new LongWritable(1));
		driver.withOutput(new Text("/foo/bar"), new LongWritable(1));
		driver.withOutput(new Text("/test"), new LongWritable(1));
		driver.runTest();
	}
}
