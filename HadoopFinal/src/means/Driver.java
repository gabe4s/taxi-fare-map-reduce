package means;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//HadoopFinal Assignment
//Author: Gabe Douda & Brad Smith
//Class: CS435

public class Driver {
	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", "\n");
		
		Job job = Job.getInstance(conf, "linreg");
		job.setJarByClass(Driver.class);
		job.setMapperClass(MR1Means.Mapper1.class);
		job.setReducerClass(MR1Means.Reducer1.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TripWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}