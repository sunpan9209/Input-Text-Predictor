import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;

/**
 * N-Gram generator
 * 
 * @author Pan
 *
 */
public class NGram {

	/**
	 * Mapper, output the n-gram and 1 as count.
	 * 
	 * @author Pan
	 *
	 */
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		private static final LongWritable count = new LongWritable(1);
		private String line;
		private Text gram = new Text();
		private String[] tokens;

		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			line = value.toString();
			if (line.length() == 0)
				return;
			line = line.trim().replaceAll("[^a-zA-Z]", " ")
					.replaceAll("\\s+", " ").toLowerCase();
			if (line != "") {
				tokens = line.trim().split(" ");
				for (int i = 0; i < tokens.length; i++) {
					StringBuilder sb = new StringBuilder();
					for (int offset = 0; offset < 5; offset++) {
						if (i + offset < tokens.length) {
							if (offset > 0) {
								sb.append(" ");
							}
							sb.append(tokens[i + offset]);
							String str = sb.toString();
							if ((str != "") && (str != " ")) {
								gram.set(str);
								output.collect(gram, count);
							}
						}
					}
				}
			}
		}
	}

	/**
	 * compute the sum of counts
	 * 
	 * @author Pan
	 *
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, LongWritable, Text, LongWritable> {
		LongWritable total = new LongWritable();

		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			long sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			total.set(sum);
			String str = key.toString();
			if ((str != "") && (str != " ")) {
				output.collect(key, total);
			}
		}
	}

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(NGram.class);
		conf.setJobName("NGram");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
}
