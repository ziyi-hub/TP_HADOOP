import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question1_8 {
	
	public enum Compteur {
		LIGNES_VIDES
	}
	
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
		
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private Counter lignesVidesCounter;

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
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
	            // Définir le mot actuel (supprime les espaces avant et après le mot actuel)
	            word.set(tokenizer.nextToken().trim());

	            // Émettre le mot comme clé de sortie et '1' comme valeur de sortie
	            context.write(word, one);
	        }

	        // Vérifier si la ligne est vide et incrémenter le compteur si c'est le cas
	        if (value.toString().trim().isEmpty()) {
	            lignesVidesCounter.increment(1);
	        }
			
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		
		Job job = Job.getInstance(conf, "Question1_8");
		job.setJarByClass(Question1_8.class);
		job.setMapperClass(MyMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
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



