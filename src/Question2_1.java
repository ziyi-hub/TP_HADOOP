import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.thirdparty.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2_1 {
	
	public static class TagMapper extends Mapper<Object, Text, Text, Text> {
		

	  
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split("\t");
	        
	        // Check if the line has 23 elements
	        if (parts.length == 23) {
	        	double latitude = Double.parseDouble(parts[10]);
	            double longitude = Double.parseDouble(parts[11]);
	            
	            // Get the country latitude and longitude
	            Country country = Country.getCountryAt(latitude, longitude);

	            if (country != null) {
            		String[] tagsTab = parts[8].split(",");
	                for(int i = 0; i < tagsTab.length; i++) {
	                	context.write(new Text(country.toString()), new Text(tagsTab[i]));
	                }
	            }
	        }
		}
	}
	
	
	public static class TagReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Count occurrences of each tag
            HashMap<String, Integer> tagCount = new HashMap<>();
            
            Configuration conf = context.getConfiguration();
            // Get the value of K from configuration
            int k = conf.getInt("topK", 3);
            
            for (Text value : values) {
                String tag = value.toString();
                tagCount.put(tag, tagCount.getOrDefault(tag, 0) + 1);
            }

            // Use a priority queue to get the top K tags
            MinMaxPriorityQueue<StringAndInt> minMaxPriorityQueue = MinMaxPriorityQueue.maximumSize(k).create();
            
            for (HashMap.Entry<String, Integer> entry : tagCount.entrySet()) {
            	minMaxPriorityQueue.add(new StringAndInt(entry.getKey(), entry.getValue())); 
            }
            
            
            

            // Emit the top K tags for the country
            while (!minMaxPriorityQueue.isEmpty()) {
            	StringAndInt tc = minMaxPriorityQueue.poll();
                context.write(key, new Text(tc.getTag() + " " + tc.getOccurrences()));
            }
        }
	}
	
	

	public static void main(String[] args) throws Exception {

		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String k = otherArgs[2];
		
        Job job = Job.getInstance(conf, "Question2_1");
        job.setJarByClass(Question2_1.class);

        job.setMapperClass(TagMapper.class);
        job.setReducerClass(TagReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        
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

        // Set the value of K as a configuration property
        job.getConfiguration().setInt("topK", Integer.parseInt(k));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
