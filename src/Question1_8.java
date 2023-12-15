import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question1_8 {
	
	public enum Compteur {
		LIGNES_VIDES
	}
	
	public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
		
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
		private Map<String, Integer> wordCountMap;
	    private Counter lignesVidesCounter;

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	        // Initialize the HashMap
	        wordCountMap = new HashMap<>();
			// Récupérer l'objet Counter pour les lignes vides
	        lignesVidesCounter = context.getCounter(Compteur.LIGNES_VIDES);
	    }

	    @Override
	    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 // Convertir le texte d'entrée en minuscules et supprimer les caractères non alphabétiques,
	        // puis diviser le texte nettoyé en mots individuels
	        StringTokenizer tokenizer = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^a-zA-Z ]", " "));

	        // Loop pour chaque mot dans le tokenizer
	        while (tokenizer.hasMoreTokens()) {
	            // Set the current word (trims leading and trailing spaces from the current word)
	            String currentWord = tokenizer.nextToken().trim();
	            
	            // Update the word count in the in-mapper combiner buffer
	            wordCountMap.put(currentWord, wordCountMap.getOrDefault(currentWord, 0) + 1);
	        }

	        // Vérifier si la ligne est vide et incrémenter le compteur si c'est le cas
	        if (value.toString().trim().isEmpty()) {
	            lignesVidesCounter.increment(1);
	        }

			
			
		}
		@Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	        // Emit the word counts from the in-mapper combiner buffer in the cleanup method
	        for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
	            word.set(entry.getKey());
	            one.set(entry.getValue());
	            context.write(word, one);
	        }
	    }
	}
	
	public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		@Override
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
		
		Job job = Job.getInstance(conf, "Question1_8_2");
		job.setJarByClass(Question1_8.class);
		job.setMapperClass(CountMapper.class);

		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setCombinerClass(CountReducer.class);
		
		job.setReducerClass(CountReducer.class);
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
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
