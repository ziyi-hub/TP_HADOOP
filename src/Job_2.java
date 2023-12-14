import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Job_2 {
		
		public static class tri extends WritableComparator {

		    // Default constructor (must call super with the class and createInstances flag)
		    public tri() {
		        super(StringAndInt2.class, true);
		    }

		    // Implement the compare method to define the sorting behavior
		    @Override
		    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//		        // Extract the occurrences field from the byte arrays and compare
//		        // This assumes a simple serialization format where occurrences is an int
		        int occurrences1 = readInt(b1, s1);
		        int occurrences2 = readInt(b2, s2);
//		        System.err.println(b1 + " " + s1 + " " + l1);
//		        // Compare in descending order based on occurrences
		        return Integer.compare(occurrences2, occurrences1);
		        
		    }
		    
//		    public int compare(WritableComparable a, WritableComparable b) {
//		        StringAndInt2 first = (StringAndInt2) a;
//				StringAndInt2 second = (StringAndInt2) b;
//
//				int occurrencesComparison = Integer.compare(second.getOccurrences(), first.getOccurrences());
//
//				if (occurrencesComparison == 0) {
//				    return first.getTag().compareTo(second.getTag());
//				}
//
//				return occurrencesComparison;
//		    }
		    
		}
		
		
		public static class Job2Mapper extends Mapper<Text, StringAndInt2, Text, StringAndInt2> {

		    @Override
		    protected void map(Text key, StringAndInt2 value, Context context) throws IOException, InterruptedException {
		        context.write(key, value);
		    }
		}

		
		
		public static class Job2Reducer extends Reducer<Text, StringAndInt2, Text, Text> {

		    public void reduce(Text key, Iterable<StringAndInt2> values, Context context) throws IOException, InterruptedException {
		        for (StringAndInt2 value : values) {
		        	context.write(key, new Text(value.getTag() + " " + value.getOccurrences()));
		        }
		    }
		}



		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        String input = otherArgs[0];
	        String output = otherArgs[1];
	        
	        Job job = Job.getInstance(conf, "Job_2");
	        job.setJarByClass(Job_2.class);

	        // Configure les classes du Mapper et du Reducer
	        job.setMapperClass(Job2Mapper.class);
	        job.setReducerClass(Job2Reducer.class);

	        // Configure les types de sortie du Mapper
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(StringAndInt2.class);

	        // Configure les types de sortie du Reducer
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);

	        // Configure les classes de format d'entrée et de sortie
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        
	        job.setSortComparatorClass(tri.class);
	        
	        // Configure les chemins d'entrée et de sortie à partir des arguments de la ligne de commande
	        FileInputFormat.addInputPath(job, new Path(input));
	        FileOutputFormat.setOutputPath(job, new Path(output));
	        
	        // Specify the output path
	 		Path outputPath = new Path(output);
	 		FileSystem fs = FileSystem.get(conf);
	 		
	 		// Check if the output directory exists
	 		if (fs.exists(outputPath)) {
	 		    // If it exists, delete it
	 		    fs.delete(outputPath, true);
	 		
	 		}

	        // Exécute le job et attend qu'il se termine
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

	}