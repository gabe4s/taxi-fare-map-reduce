package Hadoop;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// HadoopFinal Assignment
// Author: Gabe Douda & Brad Smith
// Class: CS435

public class MR2 {
	public static class Mapper2 extends Mapper<Object, Text, Text, TripWritable>{
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
				
				if(fareAmount != 0 && distance != 0 && duration != 0 && passengerCount != 0) { // Throw out any zero values
					Text writeKey = new Text(Integer.toString(rateCode));
					TripWritable writeValue = new TripWritable(fareAmount, distance, duration, passengerCount);
					
					context.write(writeKey, writeValue);
				}
				
			} catch (Exception e){
				System.out.println("MR2 Error.");
			}
		}
	}
	
	public static double getDuration(long pickup, long dropoff) {
		
		return ((double)dropoff - (double)pickup) / 60000.0; // 60000 will turn convert ms to minutes
	}
	
	public static class Reducer2 extends Reducer<Text, TripWritable, Text, TripWritable> {
		private HashMap<String, Integer> countMap = new HashMap<String, Integer>();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
	        try{
	            Path pt=new Path(context.getConfiguration().get("CountPath") + "/part-r-00000");
	            FileSystem fs = pt.getFileSystem(context.getConfiguration());
	            FSDataInputStream fsdis = fs.open(pt);
	            BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));
	            String line;
	            while((line = br.readLine()) != null && line.length() > 0) {
	            	String [] splitLine = line.split("\t");
		            countMap.put(splitLine[0], Integer.parseInt(splitLine[1]));
	            }
	        } catch(Exception e) {
	        	System.out.println("MR2 error reading count file");
	        }
		}
		
		@Override
		public void reduce(Text key, Iterable<TripWritable> values, Context context)
			throws IOException, InterruptedException {
				int entries = countMap.get(key.toString());

				double [] yFares 	= new double [entries];
				double [][] xValues = new double[entries][3];
				double [] weights	= new double[entries];
				
				int count = 0;
				for(TripWritable val : values) {
					yFares[count]     = val.getFare();
					xValues[count][0] = val.getDistance();
					xValues[count][1] = val.getDuration();
					xValues[count][2] = val.getPassengerCount();
					weights[count] 	  = 1;
					count++;
				}
				
				OLSMultipleLinearRegression mlr = new OLSMultipleLinearRegression();
				mlr.setNoIntercept(true);
				
				mlr.newSampleData(yFares, xValues);
				// [0] = intercept, [1] = x1(distance), [2] = x2(duration), [3] = x3(passengerCount)
				double [] estimates = mlr.estimateRegressionParameters();
				// double [] errors    = mlr.estimateRegressionParametersStandardErrors();

				
				context.write(key, new TripWritable(estimates[0], estimates[1], estimates[2], estimates[3]));
		}
	}
	
}
