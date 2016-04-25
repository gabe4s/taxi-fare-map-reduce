package Hadoop;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// HadoopFinal Assignment
// Author: Brad Smith
// Date: Apr 22, 2016
// Class: CS200
// Email: brad.smith.1324@gmail.com

public class MR1 {
	public static class Mapper1 extends Mapper<Object, Text, Text, TripWritable>{
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			String line      = value.toString();
			String [] tokens = line.split(",");
			try {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				long pickup   = df.parse(tokens[1]).getTime();
				long dropoff  = df.parse(tokens[2]).getTime();
				double duration = getDuration(pickup, dropoff); // trip duration in ms
				
				int passengerCount = Integer.parseInt(tokens[3]);
				float distance     = Float.parseFloat(tokens[4]);
				int   rateCode     = Integer.parseInt(tokens[7]);
				float fareAmount   = Float.parseFloat(tokens[12]);
				
				Text writeKey = new Text(Integer.toString(rateCode));
				TripWritable writeValue = new TripWritable(fareAmount, distance, duration, passengerCount);
				
				// Filter out large outliers from the data
				if(fareAmount > 0 && fareAmount < 10000 && duration > 0 && duration < 1440 
						&& distance > 0 && distance < 1000 && passengerCount > 0 && passengerCount < 10)
					context.write(writeKey, writeValue);
				
			} catch (Exception e){
				System.out.println("MR1 Error.");
			}
		}
	}
	public static double getDuration(long pickup, long dropoff) {
		// duration as minutes
		double temp = (dropoff - pickup) / 60000;
		return temp;
	}
	
	// Calculate means for each variable
	public static class Reducer1 extends Reducer<Text, TripWritable, Text, TripWritable> {
		public void reduce(Text key, Iterable<TripWritable> values, Context context)
			throws IOException, InterruptedException {
			TripWritable t = new TripWritable(0,0,0,0);
			try {
				int count = 0;
				for(TripWritable val : values){ 
					t.sum(val);
					count++;
				}
				t.average(count);
				
				context.write(key, t);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
