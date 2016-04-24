package Hadoop;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
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
	
	public static class Reducer1 extends Reducer<Text, TripWritable, Text, TripWritable> {
		public void reduce(Text key, Iterable<TripWritable> values, Context context)
			throws IOException, InterruptedException {
			Iterable <TripWritable> valuesCache = values;
			
			try {

				int count = 0;
				for(TripWritable val : valuesCache) {
					count++;
				}
				double [] fares = new double [count];
				double [][] xValues = new double[count][3];
				count = 0;
				for(TripWritable val : values) {
					fares[count]      = val.getFare();
					xValues[count][0] = val.getDistance();
					xValues[count][1] = val.getDuration();
					xValues[count][2] = val.getPassengerCount();
					count++;
				}
				
				OLSMultipleLinearRegression mlr = new OLSMultipleLinearRegression();
				
				mlr.newSampleData(fares, xValues);
				// [0] = intercept, [1] = x1, [2] = x2, [3] = x3
				double [] estimates = mlr.estimateRegressionParameters();
				double [] errors    = mlr.estimateRegressionParametersStandardErrors();
				
				context.write(key, new TripWritable(estimates[0], estimates[1], estimates[2], (int)estimates[3]));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class Reducer2 extends Reducer<Text, TripWritable, Text, TripWritable> {
		public void reduce(Text key, Iterable<TripWritable> values, Context context)
			throws IOException, InterruptedException {
			try {
				int count = 0;
				TripWritable tempTrip = new TripWritable(0,0,0,0);
				
				for(TripWritable val : values) {
					tempTrip.sum(val);
					count++;
				}
				
				tempTrip.average(count);
				context.write(key, tempTrip);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		
	}
}
