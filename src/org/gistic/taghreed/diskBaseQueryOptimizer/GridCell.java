package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.gistic.taghreed.basicgeom.MBR;

public class GridCell {
	MBR mbr; 
	HashMap<Date, Long> daysCardinality = new HashMap<Date, Long>();
	double average;
	double deviation;
	
	public GridCell(MBR mbr) {
		this.mbr = mbr;
	}
	
	public GridCell(String parsString){
		String[] token = parsString.split(";");
		this.mbr = new MBR(token[0]);
		for(int i=1;i<token.length;i++){
			String[] dayinfo = token[i].split("-");
			this.daysCardinality.put(new Date(dayinfo[0]), Long.parseLong(dayinfo[1]));
		}
	}
	
	/**
	 * This add new day and cardinality, In case there is exist day then a new value of 
	 * cardinality will be added to the old one. 
	 * @param day
	 * @param cardinality
	 */
	public void add(String day,Long cardinality){
		if(this.dayExist(new Date(day))){
   		 long newcardinality = this.getCardinality(day) + cardinality;
   		 this.daysCardinality.put(new Date(day), newcardinality);
   	 }else{
   		this.daysCardinality.put(new Date(day), cardinality);
   	 }
	}
	
	private boolean dayExist(Date day){
		return this.daysCardinality.containsKey(day);
	}
	
	public long getCardinality(String day){
		return this.daysCardinality.get(new Date(day));
	}
	
	/**
	 * This method return the average of cardinality of this cell
	 * @return
	 */
	public double getAverage() {
		Iterator it = this.daysCardinality.entrySet().iterator();
		this.average = 0;
		while(it.hasNext()){
			Map.Entry entry = (Map.Entry) it.next();
			this.average += Double.parseDouble(entry.getValue().toString());
		}
		return average/this.daysCardinality.size();
	}
	
	/**
	 * This method return the Standard deviation 
	 * A low standard deviation indicates that the data points tend to be very close to the mean 
	 * (also called expected value); 
	 * A high standard deviation indicates that the data points are spread out over a large range of values
	 * @return
	 */
	public double getDeviation() {
		Iterator it = this.daysCardinality.entrySet().iterator();
		this.average = this.getAverage();
		int  squaredDifferences = 0; 
		while(it.hasNext()){
			Map.Entry entry = (Map.Entry) it.next();
			squaredDifferences += Math.pow((Double.parseDouble(entry.getValue().toString())-this.average), 2);
		}
		this.deviation = Math.sqrt(squaredDifferences/this.daysCardinality.size());
		return deviation;
	}
	
	public MBR getMbr() {
		return mbr;
	}
	
	
	
	@Override
	public String toString() {
		StringBuilder value = new StringBuilder(); 
		value.append(this.mbr.toString());
		Iterator it = this.daysCardinality.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry entry = (Map.Entry) it.next();
			value.append(";"+entry.getKey().toString()+"-"+entry.getValue());
		}
		return value.toString();
	}
	
	

}
