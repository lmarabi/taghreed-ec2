package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.diskBaseQuery.query.Lookup;

public class GridCell {
	private static Lookup lookup;
	MBR mbr; 
	HashMap<String, Long> daysCardinality = new HashMap<String, Long>();
	private int sampleCounter;
	double average;
	double deviation;
	
	public GridCell(MBR mbr,Lookup lookup) {
		this.mbr = mbr;
		this.lookup = lookup;
	}
	
	public GridCell(String parsString,Lookup lookup){
		String[] token = parsString.split(";");
		this.mbr = new MBR(token[0]);
		for(int i=1;i<token.length;i++){
			String[] dayinfo = token[i].split("_");
			this.daysCardinality.put(dayinfo[0], Long.parseLong(dayinfo[1]));
		}
		this.lookup = lookup;
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
	 * @throws ParseException 
	 */
	public double getAverage() throws ParseException {
		Iterator it = this.daysCardinality.entrySet().iterator();
		this.average = 0;
		this.sampleCounter = this.daysCardinality.size();
		while(it.hasNext()){
			Map.Entry entry = (Map.Entry) it.next();
			if(lookup.isDayFromMissingDay((entry.getKey().toString()))){
				this.sampleCounter--;
				continue;
			}
			this.average += Long.parseLong(entry.getValue().toString());
		}
		this.average /= this.sampleCounter;
		return average;
	}
	
	/**
	 * This method return the Standard deviation 
	 * A low standard deviation indicates that the data points tend to be very close to the mean 
	 * (also called expected value); 
	 * A high standard deviation indicates that the data points are spread out over a large range of values
	 * @return
	 * @throws ParseException 
	 */
	private double getDeviation() throws ParseException {
		Iterator it = this.daysCardinality.entrySet().iterator();
		this.average = (this.average == 0) ? this.getAverage() : this.average; 
		this.deviation = 0;
		while(it.hasNext()){
			Map.Entry<String,Long> entry = (Map.Entry) it.next();
			if(lookup.isDayFromMissingDay((entry.getKey().toString()))){
				continue;
			}
			this.deviation += Math.pow((entry.getValue()-this.average), 2);
		}
		return deviation;
	}
	
	/**
	 * This method return the standard deviation:
	 * standard deviation of the sample is the degree to which individuals within the sample differ from the sample mean
	 * @return
	 * @throws ParseException 
	 */
	public double getStandardDeviation() throws ParseException{
		double result = 0;
		this.deviation = (deviation == 0) ? this.getDeviation() : this.deviation; 
		try{
		 result = (Math.sqrt(this.deviation/(this.sampleCounter-1))); 
		
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
			return (this.getStandardDeviation()/(Math.sqrt(this.sampleCounter)));
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
	
	public void getSimilarDays() throws ParseException{
		System.out.println("Sample with removing all missing days");
		List<DayCardinality> days = new ArrayList();
		
		Iterator it = this.daysCardinality.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, Long> entry = (Entry<String, Long>) it.next();
			if(lookup.isDayFromMissingDay(entry.getKey()))
				continue;
			System.out.println(entry.getKey()+"-"+entry.getValue());
			days.add(new DayCardinality(entry.getKey(), entry.getValue()));
			
		}
		System.out.println("median: "+this.getAverage()
				+"\nRelative Standard Deviation: "+this.getStandardRelativeDeviation()
				+"\nSample size:"+this.getSampleSize());
		Collections.sort(days);
		List<DayCardinality> cluster1 =  new ArrayList<DayCardinality>();
		List<DayCardinality> cluster2 =  new ArrayList<DayCardinality>();
		List<DayCardinality> outliers =  new ArrayList<DayCardinality>();
		int seed  = days.size()/4;
		System.out.println("init seed = "+seed);
		int seedcluster1 = selectRightSeed(seed, days);
		System.out.println("cluster one seed = "+ seedcluster1);
	   System.out.println("****** Cluster one **********");
		for(int i=0;i<seedcluster1;i++){
			System.out.println(days.get(i));
		}
		
		seed += seed;
		int seedCluster2 = selectRightSeed(seed, days);
		System.out.println("cluster two seed = "+ seedcluster1);
		System.out.println("****** Cluster two **********");
		for(int i=(seedcluster1+1);i<seedCluster2;i++){
			System.out.println(days.get(i));
		}
		
		System.out.println("****** cluster 3 **********");
		for(int i= (seedCluster2+1); i< days.size();i++){
			System.out.println(days.get(i));
		}
		
	}
	
	public int getSampleSize() throws ParseException{
		if(this.sampleCounter == 0 ){
			this.getAverage();
		}
		return this.sampleCounter;
	}
	
	private int selectRightSeed(int preSeed,List<DayCardinality> days){
		int test1 = days.get(preSeed).compareTo(days.get(preSeed+1));
		int test2 = days.get(preSeed+1).compareTo(days.get(preSeed+2));
		if(test1 <= test2){
			return selectRightSeed(preSeed+1, days);
			
		}else{
			return preSeed+1;
		}
	}
	
	
	
	
	
	@Override
	public String toString() {
		StringBuilder value = new StringBuilder(); 
		value.append(this.mbr.toString());
		Iterator it = this.daysCardinality.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry entry = (Map.Entry) it.next();
			value.append(";"+entry.getKey().toString()+"_"+entry.getValue());
		}
		return value.toString();
	}
	
	

}
