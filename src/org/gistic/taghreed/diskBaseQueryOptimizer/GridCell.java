package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.gistic.taghreed.basicgeom.MBR;

public class GridCell {
	MBR mbr; 
	HashMap<String, Long> daysCardinality = new HashMap<String, Long>();
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
			this.daysCardinality.put(dayinfo[0], Long.parseLong(dayinfo[1]));
		}
	}
	
	public MBR getMbr() {
		return mbr;
	}
	
	/**
	 * This add new day and cardinality, In case there is exist day then a new value of 
	 * cardinality will be added to the old one. 
	 * @param day
	 * @param cardinality
	 */
	public void add(String day,Long cardinality){
		if(this.dayExist(day)){
   		 long newcardinality = this.getCardinality(day) + cardinality;
   		 this.daysCardinality.put(day, newcardinality);
   	 }else{
   		this.daysCardinality.put(day, cardinality);
   	 }
	}
	
	private boolean dayExist(String day){
		return this.daysCardinality.containsKey(day);
	}
	
	public long getCardinality(String day){
		return this.daysCardinality.get(day);
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
			this.average += Long.parseLong(entry.getValue().toString());
		}
		return (average/this.daysCardinality.size());
	}
	
	/**
	 * This method return the Standard deviation 
	 * A low standard deviation indicates that the data points tend to be very close to the mean 
	 * (also called expected value); 
	 * A high standard deviation indicates that the data points are spread out over a large range of values
	 * @return
	 */
	private double getDeviation() {
		Iterator it = this.daysCardinality.entrySet().iterator();
		this.average = this.getAverage();
		this.deviation = 0;
		int  squaredDifferences = 0; 
		while(it.hasNext()){
			Map.Entry<String,Long> entry = (Map.Entry) it.next();
			this.deviation += Math.pow((entry.getValue()-this.average), 2);
		}
		return deviation;
	}
	
	/**
	 * This method return the standard deviation:
	 * standard deviation of the sample is the degree to which individuals within the sample differ from the sample mean
	 * @return
	 */
	public double getStandardDeviation(){
		double result = 0;
		try{
		 result = (Math.sqrt(this.getDeviation()/(this.daysCardinality.size()-1))); 
		
		}catch(Exception e){
			result = 0;
		}
		return result;
	}
	
	/**
	 * This method return the standard relative deviation
	 * @return
	 */
	public double getStandardRelativeDeviation(){
		double result = 0;
		try{
		 result = (this.getStandardDeviation()/this.average)*100; 
		
		}catch(Exception e){
			result = 0;
		}
		return result;
	}
	
	/**
	 * This method return the Standard error:
	 * standard error of the sample is an estimate of how far the sample mean is likely to be from the population mean
	 * @return
	 */
	public double getStandardError(){
		double result = 0;
		try{
			return (this.getStandardDeviation()/(Math.sqrt(this.daysCardinality.size())));
		}catch(Exception e){
			result = 0;
		}
		return result;
		
	}
	
	/**
	 * This method return the standard relative error. 
	 * @return
	 */
	public double getStandardRelativeError(){
		double result = 0;
		try{
		 result = (this.getStandardError()/this.average)*100; 
		
		}catch(Exception e){
			result = 0;
		}
		return result;
	}
	
	public String getSimilarDays(){
		String result = "";
		Iterator it = this.daysCardinality.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, Long> entry = (Entry<String, Long>) it.next();
			System.out.println(entry.getKey()+"-"+entry.getValue());
		}
		return  result;
	}
	
	public int getSampleSize(){
		return this.daysCardinality.size();
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
