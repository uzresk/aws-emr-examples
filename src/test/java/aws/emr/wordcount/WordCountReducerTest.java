package aws.emr.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountReducerTest {

    private Reducer<Text,IntWritable,Text,IntWritable>      reducer;

    private ReduceDriver<Text,IntWritable,Text,IntWritable> driver;

    @Before
    public void setUp() {
        reducer = new WordCountReducer();
        driver = new ReduceDriver<Text,IntWritable,Text,IntWritable>(
                reducer);
    }

    @Test
    public void testWordCountReduce() throws IOException{
    	// <foo : {1,1}>
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        driver.withInput(new Text("foo"), values);
        driver.withOutput(new Text("foo"), new IntWritable(2));
        List<IntWritable> values2 = new ArrayList<IntWritable>();
        values2.add(new IntWritable(2));
        values2.add(new IntWritable(1));
        driver.withInput(new Text("bar"), values2);
        driver.withOutput(new Text("bar"), new IntWritable(3));
        driver.runTest();
    }
}
