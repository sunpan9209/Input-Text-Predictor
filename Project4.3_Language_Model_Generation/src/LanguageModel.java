import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LanguageModel {

	public static class MapTask extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private String[] tokens;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//int t = Integer.parseInt(context.getConfiguration().get("t"));
			int t = 2;
			tokens = value.toString().trim().split("\t");
			long times = Long.valueOf(tokens[1]);
			if ((tokens.length > 1) && (times > t)) {
				String phrase = tokens[0];
				String[] words = phrase.split(" ");
				if (words.length <= 1) {
					return;
				}
				outputKey.set(phrase.substring(0, phrase.lastIndexOf(" ")));
				outputValue.set(words[words.length - 1] + "*" + tokens[1]);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class ReduceTask extends
			TableReducer<Text, Text, ImmutableBytesWritable> {
		String[] tokens;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int i = 0;
			String wordsRank = "";
			int size;
			//int n = Integer.parseInt(context.getConfiguration().get("n"));
			int n = 5;
			Map<String, Long> mapForSort = new HashMap<String, Long>();
			for (Text value : values) {
				tokens = value.toString().split("[*]");
				mapForSort.put(tokens[0], Long.valueOf(tokens[1]));
			}
			List<Map.Entry<String, Long>> listForSort = new ArrayList<Map.Entry<String, Long>>(
					mapForSort.entrySet());
			Collections.sort(listForSort,
					new Comparator<Map.Entry<String, Long>>() {
						public int compare(Map.Entry<String, Long> o1,
								Map.Entry<String, Long> o2) {
							int result = (int) (o2.getValue() - o1.getValue());
							return result;
						}
					});
			size = (listForSort.size() > n) ? n : listForSort.size();
			while (i < size) {
				wordsRank = listForSort.get(i).getKey() + "," + wordsRank;
			}
			if (wordsRank.length() > 0) {
				wordsRank = wordsRank
						.substring(0, wordsRank.lastIndexOf((",")));
				Put put = new Put(Bytes.toBytes(key.toString()));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("q"),
						Bytes.toBytes(wordsRank));
				if (!put.isEmpty()) {
					context.write(new ImmutableBytesWritable(key.getBytes()),
							put);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] leftArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (leftArgs.length != 4) {
			System.out.println("usage: [input] [output] [n] [threshold]");
			System.exit(-1);
		}
		Job job = new Job(conf, "model");
		job.setJarByClass(LanguageModel.class);
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//job.setNumReduceTasks(10);
		TableMapReduceUtil.initTableReducerJob("model", ReduceTask.class, job);
		FileInputFormat.addInputPath(job, new Path(leftArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(leftArgs[1]));
		conf.set("n", leftArgs[2]);
		conf.set("t", leftArgs[3]);
		job.waitForCompletion(true);
	}
}