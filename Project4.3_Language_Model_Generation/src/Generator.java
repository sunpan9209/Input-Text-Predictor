import java.io.*;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Generator {
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
	}	

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private String line;
		private String phrase, ngramTableName;
		private String[] words;
		private String[] tokens;
		private Text mapOutputKey, mapOutputValue;
		private Integer t;
		public void configure(JobConf job) {
			t = Integer.valueOf(job.get("threshold"));
		}
		/**
		 * Map function which processes each ngram count 
		 * and outputs as phrase and word with count as explained below
		 * @param key Byte number of the text in input file
		 * @param value Line containing ngram phrase and its count (separated by tab)
		 * @param output Key = first n-1 words of n-gram phrase , Value = nth word + "@" + count of the n-gram phrase 
		 * @param reporter Not used
		 * @throws IOException
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			line = value.toString().trim();
			tokens = line.split("\t");
			if(tokens.length > 1){
				if(Long.valueOf(tokens[1]) > t){
					phrase = tokens[0];
					words = phrase.split(" ");			
					//Output Key = first n-1 words and Value = last word + @ + count
					if((words.length) > 1){
						mapOutputKey = new Text(phrase.substring(0,phrase.lastIndexOf(" ")));
						mapOutputValue = new Text(words[words.length-1] + "@" + tokens[1]);
						output.collect(mapOutputKey,mapOutputValue);
					}
					//1grams are ignored as they were required only to calculate probability but 
					//as per piazza post, we can use count only to order the words. 
					//Also, 1grams are of no use as we do not need to predict anything if textbox is empty					
					//else{
					//For 1 grams output only the word and its count
					//       	mapOutputKey = new Text(words[0]);
					//	mapOutputValue = new Text(tokens[1]);
					//	output.collect(mapOutputKey,mapOutputValue);					
					//}
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		String ngramTableName;
		String[] tokens;
		WordCount wc;
		PriorityQueue<WordCount> topn ;
		//double probability;
		private Integer n;
		private HTable table;
		private Put put;
		private String words;
		//Get the value of n and initialize the HBase table
		public void configure(JobConf job) {
			n = Integer.valueOf(job.get("n"));
			try{
				table = new HTable(conf, "model");
			}catch(IOException e){
				System.err.println(e);
			}
		}
		/**
		 * WordCount class contains a word and its count. 
		 * Here word means the word followed by a phrase. 
		 * For example, in phrase "this is a","a" is the word which follows phrase "this is"
		 */
		private class WordCount implements Comparable<WordCount>{
			String word;
			Long count;
			public WordCount(String word, long count){
				this.word = word;
				this.count = count;
			}
			public boolean equals(Object other){
				if(other == null)
					return false;
				WordCount wc = (WordCount)other;
				return this.count == wc.count;
			}
			public int hashCode(){
				return count.hashCode();
			}
			public int compareTo(WordCount other){
				if(other == null)
					return 1;
				if(this.count == other.count)
					return 0;
				return this.count > other.count ? 1 : -1; 
			}
			public String toString(){
				return this.word;
			}
		}
		/**
		 * Reduce function splits the values and keeps only top n values (based on count).
		 * Then it creates a string of top n words separated by comma in order of their count i.e. 
		 * 	the word with highest count will appear as first word and so on.
		 * Then a row is inserted in HBase table with row key = phrase 
		 * 	and column value = comma separated list of words
		 * By this way, only one Put operation will happen in HBase for each row key.
		 * @param key is the phrase like "this is"
		 * @param values contains the list of words which follow key phrase with their count
		 * @param output No output - Row inserted in Hbase table
		 * @param reporter
		 * @throws IOException
		 */
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//Get the top n words from values list
			topn = new PriorityQueue<WordCount>(n);
			int i=0;
			while(values.hasNext()){
				tokens = values.next().toString().split("@");
				wc = new WordCount(tokens[0],Long.valueOf(tokens[1]));
				if(i < n){
					topn.offer(wc);
					i++;
				}else if(((WordCount)topn.peek()).count < wc.count){
					topn.poll();
					topn.offer(wc);
				}
			}
			words = "";			
			for(i=0; i<n; i++){
				if(topn.peek() == null)
					break;
				wc = topn.poll();
				words  = wc.word + " " + words;
			}
			if(words.length() > 0){
				words =	words.substring(0,words.lastIndexOf((" ")));
				//Insert the key as row key and comma separated list of words (in sorted order)
				try{
					put = new Put(Bytes.toBytes(key.toString()));
					put.add(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes(words));
					table.put(put);
				}catch(Exception e){
					System.err.println(e);
				}
			}
		}

	}
	
	public static void main(String[] args) throws IOException{
		Configuration c = new Configuration();
		String[] otherArgs = new GenericOptionsParser(c, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.out.println("usage: [input] [output] [n] [threshold]");
			System.exit(-1);
		}
		JobConf jobConf = new JobConf(Generator.class);
		jobConf.set("threshold",otherArgs[3]);
		jobConf.set("n",otherArgs[2]);
		jobConf.setJobName("Generator");
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
	//	jobConf.setNumMapTasks(20);
	//	jobConf.setNumReduceTasks(20);
		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobConf, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(otherArgs[1]));

		JobClient.runJob(jobConf);
		System.exit(0);
	}
}
