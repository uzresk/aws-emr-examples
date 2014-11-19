package aws.emr.accesslog;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AccessLogMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	private final static LongWritable one = new LongWritable(1);
	private Text url = new Text();
	
	private static final Pattern PATTERN = Pattern
			.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		Matcher matcher = PATTERN.matcher(line);
		if (!matcher.matches()) {
			System.err.println("no matches");
		}
		// System.out.println("IP:" + matcher.group(0));
		// System.out.println("Time:" + matcher.group(4));
		String path = matcher.group(5).split(" ")[1];
		System.out.println("URL:" + path);
		url.set(path);
		context.write(url, one);
	}

}
