package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.util.List;

public class Cluster {
	private List<DayCardinality> days;
	private double mean;
	
	public Cluster() {
		// TODO Auto-generated constructor stub
	}
	
	public List<DayCardinality> getDays() {
		return days;
	}
	
	public void addToCluster(DayCardinality day) {
		this.days.add(day);
	}
	
	/**
	 * This method return the mean of the cluster. 
	 * @return
	 */
	public double getMean() {
		int sum = 0; 
		for(int i=0 ; i < days.size() ; i++){
			sum += days.get(i).getCardinality();
		}
		double avg = sum/days.size();
		return avg;
	}
	
	

}
