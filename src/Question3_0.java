import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

public class Question3_0 {
	

	public static class TagMapper extends Mapper<Object, Text, Text, StringAndInt> {
		

	  
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split("\t");
	        
	        // Check if the line has 23 elements
	        if (parts.length == 23) {
	        	if (!parts[10].isEmpty() && !parts[11].isEmpty() && !parts[8].isEmpty()) {
	        		double latitude = Double.parseDouble(parts[11]);
		            double longitude = Double.parseDouble(parts[10]);
		            
		            // Get the country latitude and longitude
		            Country country = Country.getCountryAt(latitude, longitude);

		            if (country != null) {
	            		String[] tagsTab = parts[8].split(",");
		                for(int i = 0; i < tagsTab.length; i++) {
		                	context.write(new Text(country.toString()), new StringAndInt(tagsTab[i], 1));
		                }
		            }
	        	}
	        	
	        }
		}
	}
	
	
	public static class TagReducer extends Reducer<Text, StringAndInt, Text, StringAndInt> {

        public void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
            // Count occurrences of each tag
            HashMap<String, Integer> tagCount = new HashMap<>();
            
            Configuration conf = context.getConfiguration();			
            // Get the value of K from configuration
            int k = conf.getInt("topK", 5);
            
            for (StringAndInt value : values) {
                String tag = value.getTag();
                int occurrences = value.getOccurrences();
                tagCount.put(tag, tagCount.getOrDefault(tag, 0) + occurrences);
            }

            // Use a priority queue to get the top K tags
            MinMaxPriorityQueue<StringAndInt> minMaxPriorityQueue = MinMaxPriorityQueue.maximumSize(k).create();
            
            for (HashMap.Entry<String, Integer> entry : tagCount.entrySet()) {
            	minMaxPriorityQueue.add(new StringAndInt(entry.getKey(), entry.getValue())); 
            }
          
            // Emit the top K tags for the country
            for (StringAndInt tc: minMaxPriorityQueue) {
                context.write(key, tc);
            }
         
        }
	}
	
	public static class TagCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {

        @Override
        protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
        	HashMap<String, Integer> tagCount = new HashMap<>();
        	// Count occurrences of each tag for the current country
        	for (StringAndInt value : values) {
                String tag = value.getTag();
                int occurrences = value.getOccurrences();
                tagCount.put(tag, tagCount.getOrDefault(tag, 0) + occurrences);
            }

        	for (Map.Entry<String, Integer> entry : tagCount.entrySet()) {
                StringAndInt result = new StringAndInt(entry.getKey(), entry.getValue());
                context.write(key, result);
            }
        }
    }
	
	
	
	
	

	public static void main(String[] args) throws Exception {

		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String k = otherArgs[2];
		
        Job job = Job.getInstance(conf, "Question3_0");
        job.setJarByClass(Question3_0.class);

        job.setMapperClass(TagMapper.class);
        job.setCombinerClass(TagCombiner.class);
        job.setReducerClass(TagReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringAndInt.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringAndInt.class);

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
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Set the value of K as a configuration property
        job.getConfiguration().setInt("topK", Integer.parseInt(k));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
