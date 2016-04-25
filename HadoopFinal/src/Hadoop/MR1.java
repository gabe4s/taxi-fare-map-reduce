package Hadoop;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// HadoopFinal Assignment
// Author: Gabe Douda & Brad Smith
// Class: CS435

public class MR1 {
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			String line      = value.toString();
			String [] tokens = line.split(",");
			try {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				long pickup   = df.parse(tokens[1]).getTime();
				long dropoff  = df.parse(tokens[2]).getTime();
				
				double duration 	  = getDuration(pickup, dropoff); // trip duration in minutes
				double passengerCount = Double.parseDouble(tokens[3]);
				double distance       = Double.parseDouble(tokens[4]);
				int   rateCode     	  = Integer.parseInt(tokens[7]);
				double fareAmount     = Double.parseDouble(tokens[12]);
				
				if(fareAmount > 0 && fareAmount < 100 && distance > 0 && distance < 50 && duration > 0 && duration < 60 && passengerCount > 0 && passengerCount < 10) { // Throw out any zero values
					Text writeKey = new Text(Integer.toString(rateCode));
					
					context.write(writeKey, new IntWritable(1));
				}
				
			} catch (Exception e){
				System.out.println("MR1 Error.");
			}
		}
		
		public static double getDuration(long pickup, long dropoff) {
			
			return ((double)dropoff - (double)pickup) / 60000.0; // 60000 will turn convert ms to minutes
		}
	}
	
	
	
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			
				int count = 0;
				for(IntWritable val : values) {
					count++;
				}

				context.write(key, new IntWritable(count));
		}
	}
	
}
