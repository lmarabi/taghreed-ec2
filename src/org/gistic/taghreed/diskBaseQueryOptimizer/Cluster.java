package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Cluster {
	private List<DayCardinality> days;
	private double mean;
	private double confidenceLevel;
	
	public Cluster() {
		// TODO Auto-generated constructor stub
		this.days = new ArrayList<DayCardinality>();
		this.mean = 0; 
		this.confidenceLevel = 0;
	}
	
	public Cluster(String ParseString){
		this.days = new ArrayList<DayCardinality>();
		String[] token = ParseString.split("_");
		for(int i=0;i<token.length-2;i++){
			this.days.add(new DayCardinality(token[i], 0));
		}
		this.mean = Double.parseDouble(token[token.length-2]);
		this.confidenceLevel = Double.parseDouble(token[token.length-1]);
	}
	
	public List<DayCardinality> getDays() {
		return days;
	}
	
	/**
	 * This method check if the day is in the cluster. 
	 * @param day
	 * @return
	 */
	public boolean contains(String day){
		for(DayCardinality d : this.days){
			if(d.getDay().equals(day))
				return true;
		}
		return false;
	}
	
	public void addToCluster(DayCardinality day) {
		this.days.add(day);
	}
	
	public void setMean(double mean) {
		this.mean = mean;
	}
	
	/**
	 * This method return the mean of the cluster. 
	 * @return
	 */
	public double getMean() {
		return ConfidenceCoefficient.getMeanOfCluster(this);
	}
	
	public double getConfidence(){
		return ConfidenceCoefficient.getConfidenceOfCluster(this);
	}
	
	/**
	 * This method return the mean of the cluster. 
	 * @return
	 */
	public double getOverallMean() {
		return this.mean;
	}
	
	public double getOverallConfidence(){
		return this.confidenceLevel;
	}
	
	@Override
	public String toString() {
		StringBuilder value = new StringBuilder();
		for(DayCardinality dayCard : this.days){
			value.append(dayCard.getDay()+"_");
		}
		value.append(this.getMean()+"_"+this.getConfidence());
		return value.toString();
	}
	

}
