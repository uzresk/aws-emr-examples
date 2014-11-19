package aws.emr.wordcount;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountMapperTest extends TestCase {

	private Mapper<LongWritable, Text, Text, IntWritable> mapper;

	private MapDriver<LongWritable, Text, Text, IntWritable> driver;

	@Before
	public void setUp() {

		mapper = new WordCountMapper();
		driver = new MapDriver<LongWritable, Text, Text, IntWritable>(mapper);
	}

	@Test
	public void testCount() throws IOException {
		
		driver.withInput(new LongWritable(1), new Text(
				"foo bar foo bar hoge"));
		driver.withInput(new LongWritable(1), new Text(
				"foo"));		
		driver.withOutput(new Text("foo"), new IntWritable(1));
		driver.withOutput(new Text("bar"), new IntWritable(1));
		driver.withOutput(new Text("foo"), new IntWritable(1));
		driver.withOutput(new Text("bar"), new IntWritable(1));
		driver.withOutput(new Text("hoge"), new IntWritable(1));
		driver.withOutput(new Text("foo"), new IntWritable(1));
		driver.runTest();
	}
}
