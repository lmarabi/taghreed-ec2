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
	private List<Cluster> cluster;
	
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
	public void add(String day,long cardinality){
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
			this.average += Integer.parseInt(entry.getValue().toString());
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
	
	/**
	 * This method cluster the data in the grid cell. 
	 * @return number of group clusters created in the this Grid cell 
	 * @throws ParseException
	 */
	public int initCluster() throws ParseException{
		System.out.println("Sample with removing all missing days");
		List<DayCardinality> days = new ArrayList();
		Iterator it = this.daysCardinality.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, Integer> entry = (Entry<String, Integer>) it.next();
			if(lookup.isDayFromMissingDay(entry.getKey()))
				continue;
			System.out.println(entry.getKey()+"-"+entry.getValue());
			days.add(new DayCardinality(entry.getKey(), entry.getValue()));
			
		}
		System.out.println("median: "+this.getAverage()
				+"\nRelative Standard Deviation: "+this.getStandardRelativeDeviation()
				+"\nSample size:"+this.getSampleSize());
		if(this.getStandardRelativeDeviation() > 5){
			return 0;
		}
		//Sort the list of days
		Collections.sort(days);
		// Create the cluster
		return createClusts(days);
	}
	
	/***
	 * This method take a list of days in the histogram cell and return it clustered based on their Cardinality 
	 * Each group in cluster the standard relative deviation is less than or equal to 4.0% 
	 * @param list
	 * @return number of group clusters created 
	 */
	private int createClusts(List<DayCardinality> list){
		List<Integer> seeds = new ArrayList<Integer>();
		this.cluster = new ArrayList<Cluster>();
		int previous = 0;
		for(int i=0 ; i < list.size()-1 ; i++){
			
			//( | V1 - V2 | / ((V1 + V2)/2) ) * 100 
			int V1 = list.get(i).getCardinality();
			int V2 = list.get(i+1).getCardinality();
			int preValue = list.get(previous).getCardinality();
			double gapInc = calculateGapPercentage(V1, V2); 
			double gapPrevious = calculateGapPercentage(preValue, V2);
			if(gapInc > 15 || gapPrevious >15){
				seeds.add(i+1);
				previous = i+1;
			}
		}
		
		System.out.println("Number of clusters found: "+seeds.size());
		//Add the first clusters to the list. 
		Cluster clusterobject = new Cluster();
		for(int i= 0 ; i < seeds.get(0) ; i++){
			clusterobject.addToCluster(list.get(i));
		}
		//add the cluster object to the list of clusters.
		this.cluster.add(clusterobject);
		// Add the intermediate clusters objects. 
		for(int seed =0 ; seed < seeds.size()-1; seed++){
			clusterobject = new Cluster();
			for(int i= seeds.get(seed) ; i< seeds.get(seed+1) ; i++){
				clusterobject.addToCluster(list.get(i));
			}
			this.cluster.add(clusterobject);
		}
		
		//Add the last cluster to the list
		clusterobject = new Cluster();
		for(int i= seeds.get(seeds.size()-1) ; i < list.size() ; i++){
			clusterobject.addToCluster(list.get(i));
		}
		this.cluster.add(clusterobject);
		
		return this.cluster.size();
	}
	
	/***
	 * This method calculate the percentage gap between two number 
	 * @param value1
	 * @param value2
	 * @return double % if it is greater than 15% it mean that the standard relative deviation is grater than 4.0% 
	 */
	private double calculateGapPercentage(int value1, int value2){
		int x =  Math.abs((value1 - value2));
		double y = (value1 + value2);
		y /= 2;
		double gap = x /y ;
		gap *= 100;
		System.out.println("v1:"+value1+" v2:"+value2+" gap:"+gap);
		return gap;
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
