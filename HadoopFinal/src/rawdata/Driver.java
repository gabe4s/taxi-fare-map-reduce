package rawdata;
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

public class Driver {
	
	private final static int NUM_REDUCE_TASKS = 1;
	
	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf1 = new Configuration();
		conf1.set("textinputformat.record.delimiter", "\n");
		
		Job job1 = Job.getInstance(conf1, "TaxiFareRawData");
		job1.setNumReduceTasks(NUM_REDUCE_TASKS);
		job1.setJarByClass(Driver.class);
		job1.setMapperClass(MR1RawData.Mapper1.class);
		job1.setReducerClass(MR1RawData.Reducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(TripWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
	}
}
