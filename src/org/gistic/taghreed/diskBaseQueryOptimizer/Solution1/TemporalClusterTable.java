package org.gistic.taghreed.diskBaseQueryOptimizer.Solution1;

import java.util.List;

public class TemporalClusterTable {
	int id; 
	List<Bucket> histogram; 
	
	public TemporalClusterTable() {
		// TODO Auto-generated constructor stub
	}
	
	public TemporalClusterTable(int id, List<Bucket> buckets) {
		this.id = id; 
		this.histogram = buckets;
	}
	
	public List<Bucket> getHistogram() {
		return histogram;
	}
	
	public void setHistogram(List<Bucket> histogram) {
		this.histogram = histogram;
	}
	
	// implement the methods required to intersect and return the total cost.etc. 

}
