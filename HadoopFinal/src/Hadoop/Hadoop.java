package Hadoop;
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

// AWS Run Args: Hadoop.Hadoop s3://taxifare435/*.csv s3://taxifare435/Out/

public class Hadoop {
	
	private final static int NUM_REDUCE_TASKS = 1;
	private final static String COUNT_PATH_ADDITION = "Count";
	
	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf1 = new Configuration();
		conf1.set("textinputformat.record.delimiter", "\n");
		
		Job job1 = Job.getInstance(conf1, "TaxiFareCount");
		job1.setNumReduceTasks(1); // Needs to be 1 here for a single count file
		job1.setJarByClass(Hadoop.class);
		job1.setMapperClass(MR1.Mapper1.class);
		job1.setReducerClass(MR1.Reducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + COUNT_PATH_ADDITION));
		job1.waitForCompletion(true);
		
		System.gc();
		
		Configuration conf2 = new Configuration();
		conf2.set("textinputformat.record.delimiter", "\n");
		conf2.set("CountPath", args[1] + COUNT_PATH_ADDITION);
		
		Job job2 = Job.getInstance(conf2, "TaxiFareLinearRegression");
		job2.setNumReduceTasks(NUM_REDUCE_TASKS);
		job2.setJarByClass(Hadoop.class);
		job2.setMapperClass(MR2.Mapper2.class);
		job2.setReducerClass(MR2.Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(TripWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "LinearRegression"));
		job2.waitForCompletion(true);
	}
}
