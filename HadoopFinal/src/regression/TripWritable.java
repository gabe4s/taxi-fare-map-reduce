package regression;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// HadoopFinal Assignment
// Author: Gabe Douda & Brad Smith
// Class: CS435

public class TripWritable implements Writable{
	private double fare;
	private double distance;
	private double duration;
	private double passengerCount;
	
	public TripWritable(){}
	
	public TripWritable(double fare, double distance, double duration, double passengerCount){
		setFare          (fare);
		setDistance      (distance);
		setDuration      (duration);
		setPassengerCount(passengerCount);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		fare     	   = in.readDouble();
		distance 	   = in.readDouble();
		duration 	   = in.readDouble();
		passengerCount = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(fare);
		out.writeDouble(distance);
		out.writeDouble(duration);
		out.writeDouble(passengerCount);
	}
	
	public void sum(TripWritable t) {
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
		return "y = " + getFare() + " + " + getDistance() + "x1 + " + getDuration() + "x2 + " + getPassengerCount() + "x3" ;
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
	
	
}
