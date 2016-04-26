package regression;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// HadoopFinal Assignment
// Author: Gabe Douda & Brad Smith
// Class: CS435


public class Driver {
	
	private final static int NUM_REDUCE_TASKS = 1;
	private final static String COUNT_PATH_ADDITION = "Count"; // Make sure it is same as constant in count job
	
	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", "\n");
		conf.set("CountPath", args[1] + COUNT_PATH_ADDITION);
		
		Job job = Job.getInstance(conf, "TaxiFareLinearRegression");
		job.setNumReduceTasks(NUM_REDUCE_TASKS);
		job.setJarByClass(Driver.class);
		job.setMapperClass(MR1Regression.Mapper1.class);
		job.setReducerClass(MR1Regression.Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(RegressionWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "LinearRegression"));
		job.waitForCompletion(true);
	}
}
