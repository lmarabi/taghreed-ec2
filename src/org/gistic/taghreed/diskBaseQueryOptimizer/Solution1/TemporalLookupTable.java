package org.gistic.taghreed.diskBaseQueryOptimizer.Solution1;

public class TemporalLookupTable {
	String temporalSegment; 
	long volume; 
	int clusterId; 
	
	public TemporalLookupTable() {
		// TODO Auto-generated constructor stub
	}
	
	public TemporalLookupTable(String temporal,long volume, int clusterId){
		this.temporalSegment = temporal; 
		this.clusterId = clusterId;
		this.volume = volume;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.temporalSegment+"\t"+this.volume+"\t"+this.clusterId;
	}

}
