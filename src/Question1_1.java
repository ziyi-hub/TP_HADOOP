import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question1_1 {
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	   
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Convert the input text to lowercase and remove non-alphabetic characters, break the cleaned text into individual words
	        StringTokenizer tokenizer = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^a-zA-Z ]", " "));
	        
	     // Loop iterates through each word in the tokenizer
	        while (tokenizer.hasMoreTokens()) {		        
		        // Set the current word (trims leading and trailing spaces from the current word)
	            word.set(tokenizer.nextToken().trim());
	            
	            // Emit the word as the output key and '1' as the output value <"i", 1> <"h", 1> <"h", 1>
	            context.write(word, one);
	        }
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		protected void reduce(Text word, Iterable<IntWritable> occurrences, Context output) throws IOException, InterruptedException {
			
			int count = 0;
			
			//This loop iterates over the values associated with a particular key. 
			//In the context of a word count, each value is the number 1. It adds up all these 1 values to calculate the total count (sum) for the current word.
			for(IntWritable occ : occurrences) {
				count += occ.get();
			}
			result.set(count);
			output.write(word, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		
		Job job = Job.getInstance(conf, "Question1_1");
		job.setJarByClass(Question1_1.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		// Specify the output path
		Path outputPath = new Path(output);
		FileSystem fs = FileSystem.get(conf);
		
		// Check if the output directory exists
		if (fs.exists(outputPath)) {
		    // If it exists, delete it
		    fs.delete(outputPath, true);
		}
		
		// Set the output path for the job
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
