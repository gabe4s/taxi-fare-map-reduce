package Hadoop;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// HadoopFinal Assignment
// Author: Brad Smith
// Date: Apr 22, 2016
// Class: CS200
// Email: brad.smith.1324@gmail.com

public class TripWritable implements Writable{
	private double fare, distance, duration, passengerCount;
	
	public TripWritable(){}
	
	public TripWritable(double fare, double distance, double duration, double passengerCount){
		this.setFare          (fare);
		this.setDistance      (distance);
		this.setDuration      (duration);
		this.setPassengerCount(passengerCount);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		fare     = in.readDouble();
		distance = in.readDouble();
		duration = in.readDouble();
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
		fare     += t.getFare();
		distance += t.getDistance();
		duration += t.getDuration();
		passengerCount += t.getPassengerCount();
	}
	
	public void average(int ni) {
		double n = (double) ni;
		fare     = fare / n;
		distance = distance / n;
		duration = duration / n;
		passengerCount = passengerCount / n;
	}
	
	@Override
	public String toString(){
		return fare + "\t" + distance + "\t" + duration + "\t" + passengerCount ;
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
