package regression;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// HadoopFinal Assignment
// Author: Gabe Douda & Brad Smith
// Class: CS435

public class RegressionWritable implements Writable{
	private double fare;
	private double distance;
	private double duration;
	private double passengerCount;
	private double interceptError;
	private double x1Error;
	private double x2Error;
	private double x3Error;
	
	public double getInterceptError() {
		return interceptError;
	}

	public RegressionWritable(){}
	
	public RegressionWritable(double fare, double distance, double duration, double passengerCount, double interceptError, double x1Error, double x2Error, double x3Error){
		setFare          (fare);
		setDistance      (distance);
		setDuration      (duration);
		setPassengerCount(passengerCount);
		setInterceptError(interceptError);
		setX1Error		 (x1Error);
		setX2Error		 (x2Error);
		setX3Error		 (x3Error);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		fare     	   = in.readDouble();
		distance 	   = in.readDouble();
		duration 	   = in.readDouble();
		passengerCount = in.readDouble();
		interceptError = in.readDouble();
		x1Error	   	   = in.readDouble();
		x2Error 	   = in.readDouble();
		x3Error 	   = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(fare);
		out.writeDouble(distance);
		out.writeDouble(duration);
		out.writeDouble(passengerCount);
		out.writeDouble(interceptError);
		out.writeDouble(x1Error);
		out.writeDouble(x2Error);
		out.writeDouble(x3Error);
	}
	
	public void sum(RegressionWritable t) {
		fare 		   += t.getFare();
		distance 	   += t.getDistance();
		duration 	   += t.getDuration();
		passengerCount += t.getPassengerCount();
	}
	
	public void average(int n) {
		fare     	   /= n;
		distance 	   /= n;
		duration 	   /= n;
		passengerCount /= n;
	}
	
	@Override
	public String toString(){
		return "y = " + getFare() + " + " + getDistance() + "x1 + " + getDuration() + "x2 + " + getPassengerCount() + "x3\tERRORS: Intercept: " + getInterceptError() + " x1: " + getX1Error() + " x2: " + getX2Error() + " x3: " + getX3Error();
	}

	public double getDuration() {
		return duration;
	}

	public void setDuration(double duration) {
		this.duration = duration;
	}

	public double getFare() {
		return fare;
	}

	public void setFare(double fare) {
		this.fare = fare;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

	public double getPassengerCount() {
		return passengerCount;
	}

	public void setPassengerCount(double passengerCount) {
		this.passengerCount = passengerCount;
	}
	
	public void setInterceptError(double interceptError) {
		this.interceptError = interceptError;
	}

	public double getX1Error() {
		return x1Error;
	}

	public void setX1Error(double x1Error) {
		this.x1Error = x1Error;
	}

	public double getX2Error() {
		return x2Error;
	}

	public void setX2Error(double x2Error) {
		this.x2Error = x2Error;
	}

	public double getX3Error() {
		return x3Error;
	}

	public void setX3Error(double x3Error) {
		this.x3Error = x3Error;
	}
	
	
}
