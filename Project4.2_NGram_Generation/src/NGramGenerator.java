import java.io.*;
import java.util.*;
import java.nio.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class NGramGenerator {
	public static void main(String[] args) throws IOException{
		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}
		JobConf conf = new JobConf(NGramGenerator.class);
		conf.setJobName("NGramGenerator");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		System.exit(0);
	}
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
		private Text phrase = new Text();
		private static final LongWritable count = new LongWritable(1);
		private String line;
		private ArrayList<String> ngrams;
		private String[] tokens;
		private String twogram, threegram, fourgram, fivegram; 
		//Map function outputs the n-gram phrase and 1 as count
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			line = value.toString();
			line = line.replaceAll("[^a-zA-Z]", " ").replaceAll("\\s+"," ").toLowerCase();
			if(line != ""){
                                ngrams = get5grams(line);
                                for(String ngram : ngrams){
                                	phrase.set(ngram);
                                        output.collect(phrase,count);
                                }
                        }
		}
		public ArrayList<String> get5grams(String input){
                        ArrayList<String> list = new ArrayList<String>();
                        tokens = input.split(" ");
                        for(String token : tokens)
                                list.add(token);
                        int n = list.size();
                        for(int i=0; i+1 < n; i++){
                                twogram = list.get(i) + " " + list.get(i+1);
                                list.add(twogram);
                                if(i+2 < n){
                                        threegram = twogram + " " + list.get(i+2);
                                        list.add(threegram);
                                        if(i+3 < n){
                                                fourgram = threegram + " " + list.get(i+3);
                                                list.add(fourgram);
                                                if(i+4 < n){
                                                        fivegram = fourgram + " " + list.get(i+4);
                                                        list.add(fivegram);
                                                }
                                        }

                                }
                        }
                        return list;
                }

	}
	//Reduce function computes the sum of the counts and outputs total for each ngram
	public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
		LongWritable total = new LongWritable();
		public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			long sum = 0;
			while(values.hasNext()){
				sum += values.next().get();				
			}
			total.set(sum);
			output.collect(key, total);
		}
	}
}
