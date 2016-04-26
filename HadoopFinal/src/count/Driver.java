package count;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// HadoopFinal Assignment
// Author: Gabe Douda & Brad Smith
// Class: CS435


public class Driver {
	
	private final static String COUNT_PATH_ADDITION = "Count";
	
	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf1 = new Configuration();
		conf1.set("textinputformat.record.delimiter", "\n");
		
		Job job1 = Job.getInstance(conf1, "TaxiFareCount");
		job1.setNumReduceTasks(1); // Needs to be 1 here for a single count file
		job1.setJarByClass(Driver.class);
		job1.setMapperClass(MR1Count.Mapper1.class);
		job1.setReducerClass(MR1Count.Reducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + COUNT_PATH_ADDITION));
		job1.waitForCompletion(true);

	}
}
