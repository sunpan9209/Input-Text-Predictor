import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;

/**
 * build line index without removing stop words.
 * 
 * @author Pan
 *
 */
public class Indexer {

	/**
	 * Mapper
	 * 
	 * @author Pan
	 *
	 */
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text input = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
			String inputFileName = fileSplit.getPath().getName();
			input.set(inputFileName);
			String line = value.toString().toLowerCase();

			// stripping punctuation
			line = line.replaceAll("[^0-9a-z]", " ");
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, input);
			}
		}
	}

	/**
	 * reducer
	 * 
	 * @author Pan
	 *
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			HashSet<String> files = new HashSet<String>();
			StringBuilder returnValue = new StringBuilder();
			while (values.hasNext()) {
				files.add(values.next().toString());
			}
			for (String file : files) {
				returnValue.append(file);
				returnValue.append(" ");
			}
			output.collect(key, new Text(returnValue.toString()));
		}
	}

	/**
	 * map reduce driver
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(Indexer.class);
		conf.setJobName("Indexer");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		// conf.setNumMapTasks(8);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
}
