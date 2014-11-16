import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;

/**
 * build line index with removing stop words.
 * 
 * @author Pan
 *
 */
public class Bonus {
	private static String stopWordsList = "/stopword.txt";

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
		private Set<String> stopWords = new HashSet<String>();

		/**
		 * Read stop words file from distributed cache
		 */
		public void configure(JobConf job) {
			String stopWord = null;
			try {
				Path[] cacheFilePath = DistributedCache.getLocalCacheFiles(job);
				for (Path stopWordsFile : cacheFilePath) {
					@SuppressWarnings("resource")
					BufferedReader reader = new BufferedReader(new FileReader(
							stopWordsFile.toString()));
					while ((stopWord = reader.readLine()) != null) {
						stopWords.add(stopWord);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

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
			String token = null;
			while (tokenizer.hasMoreTokens()) {
				token = tokenizer.nextToken();
				if (!stopWords.contains(token)) {
					word.set(token);
					output.collect(word, input);
				}
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
		JobConf conf = new JobConf(Bonus.class);
		conf.setJobName("Bonus");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		DistributedCache.addCacheFile(new Path(stopWordsList).toUri(), conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
}
