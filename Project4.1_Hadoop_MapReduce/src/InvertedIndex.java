import java.io.*;
import java.util.*;
import java.nio.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
//This version of class builds the inverted index without using stop words.
public class InvertedIndex {
	public static void main(String[] args) throws IOException{
		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}
		JobConf conf = new JobConf(InvertedIndex.class);
		conf.setJobName("invertedIndex");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combine.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setNumMapTasks(8);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		System.exit(0);
	}
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text inputFile = new Text();
		public void configure(JobConf job) {
			String inputFileName = job.get("map.input.file");
			inputFile.set(inputFileName.substring(inputFileName.lastIndexOf("/")+1));
		}
		//Map function outputs the word and input file name in which it occurs
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString().toLowerCase();
			//Remove non-alphanumeric chars
			line = line.replaceAll("[^a-z0-9]", " ");
			StringTokenizer tokenizer = new StringTokenizer(line);
			String token;
			while(tokenizer.hasMoreTokens()){
				token = tokenizer.nextToken();
				word.set(token);
				output.collect(word, inputFile);
			}
		}
	}
	public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		//Combine function outputs the word and list of space separated filenames
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			HashSet<String> files = new HashSet<String>();
			while(values.hasNext()){
				files.add(values.next().toString());				
			}
			StringBuilder sb = new StringBuilder();			
			for(String file : files){
				sb.append(file);
				sb.append(" ");
			}			
			output.collect(key, new Text(sb.toString()));
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		//Reduce function outputs the word and list of space separated filenames.
		//Its input value may be space separated list of files (because of combine)
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			HashSet<String> files = new HashSet<String>();
			String[] fileNameArray = null;
			while(values.hasNext()){
				fileNameArray = values.next().toString().split(" ");
				for(String f : fileNameArray)
					files.add(f);				
			}
			StringBuilder sb = new StringBuilder();	
			for(String file : files){
				sb.append(file);
				sb.append(" ");
			}
			output.collect(key, new Text(sb.toString()));
		}
	}
}
