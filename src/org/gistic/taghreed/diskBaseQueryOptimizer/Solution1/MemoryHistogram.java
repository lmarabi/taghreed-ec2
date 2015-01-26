package org.gistic.taghreed.diskBaseQueryOptimizer.Solution1;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.gistic.taghreed.Commons;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.Partition;
import org.gistic.taghreed.collections.Week;
import org.gistic.taghreed.diskBaseQuery.query.Lookup;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryIndex;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQueryOptimizer.GridCell;
import org.gistic.taghreed.diskBaseQueryOptimizer.HistogramCluster;
import org.gistic.taghreed.diskBaseQueryOptimizer.TraditionalMultiHistogram;


/**
 * The idea of this solution as follow , 
 * 
 * we list all the days with the volume, and construct the histogram based only on the the present
 * -----------
 * | day-volume-clusterid  | , | histogram | 
 * 
 * the algorithm as follow:
 * 1- find the max and the min (x,y) 
 * 2- construct a fixd withd bucket width divide the max-min\10,000 this way we construct grid like structure for each day 
 * 3- fill the buckets of each day with the persent of the data in that day. 
 * 4- find histogram that look similar to each other. this we will end by haveing n number of clusters 
 * 5- combine buckets in the histogram to save more space in the histogram. the same idea we discuss.  
 * @author louai
 *
 */



public class MemoryHistogram {
	
	private List<HistogramCluster> cluster = new ArrayList<HistogramCluster>();
	
	String startDay;
	String endDay;
	GridCell worldCell;
	HashMap<String,HistogramCluster> dayHistogram;
	HashMap<String,HistogramCluster> weekHistogram;
	HashMap<String,HistogramCluster> monthHistogram;
	queryLevel level;
	Lookup lookup;
	
	//MBR of the grid cell 
	Point maxPoint;
	Point minPoint;
	Map<String,List<Bucket>> initHistogram; 
	List<Bucket> histogramBackets; 
//	Map<String,List<String>> clusters;
	
	List<TemporalLookupTable> dayLookup;
	List<TemporalClusterTable> dayClusters;
	

	public MemoryHistogram() throws IOException, ParseException {
		// init in memory histograms
		ServerRequest server = new ServerRequest();
		server.setIndex(queryIndex.rtree);
		this.lookup = server.getLookup();
		this.dayHistogram = new HashMap<String, HistogramCluster>();
		this.weekHistogram = new HashMap<String, HistogramCluster>();
		this.monthHistogram = new HashMap<String, HistogramCluster>();
		// Offline processing
		this.histogramBackets = new ArrayList<Bucket>();
		this.initHistogram = new HashMap<String, List<Bucket>>();
		this.dayClusters = new ArrayList<TemporalClusterTable>();
		this.dayLookup = new ArrayList<TemporalLookupTable>();
		 OfflinePhase();
	}

	/**
	 * This method is offline phase of to prepare the clusters of the histogram.
	 * 
	 * @throws IOException
	 * @throws ParseException 
	 */
	private void OfflinePhase() throws IOException, ParseException {
		double temp,maxlon = 0,maxlat = 0;
		double minlat = Double.MAX_VALUE,minlon = Double.MAX_VALUE;
		MBR mbr = new MBR(new Point(90, 180), new Point(-90, -180));
		String startDate = "2000-01-01";
		String endDate = "3000-01-01";
		Map<String,String> days = this.lookup.getTweetsDayIndex(startDate, endDate);
		Iterator it = days.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, String> obj = (Entry<String, String>) it.next();
			dayHistogram.put(obj.getKey(), new HistogramCluster(obj.getValue()));
			//Get the maximum and the Minimum MBR over all paritions 
			temp = dayHistogram.get(obj.getKey()).getMinLon();
			minlon = temp < minlon ? temp : minlon;
			temp = dayHistogram.get(obj.getKey()).getMinLat();
			minlat = temp < minlat ? temp : minlat;
			temp = dayHistogram.get(obj.getKey()).getMaxlat();
			maxlat = temp > maxlat ? temp : maxlat;
			temp = dayHistogram.get(obj.getKey()).getMaxLon();
			maxlon = temp > maxlon ? temp : maxlon;
			
		}
		// We partition the space of the histograms into buckets of equal width size. 
		maxPoint = new Point(maxlat, maxlon);
		minPoint = new Point(minlat, minlon);
		histogramBackets = creatHistogramBuckets(maxPoint,minPoint);
		initHistogramo(queryLevel.Day);
		matchDays();
		
		System.out.println("**** Day level**** with size:"+ dayLookup.size());
		for(TemporalLookupTable day : dayLookup){
			System.out.println(day.toString());
		}
		
		System.out.println("\n*****\nNumber of clusters:  "+dayClusters.size());
		
//		Map<Week,String> week = this.lookup.getAllTweetsWeekIndex(startDate, endDate);
//		it = week.entrySet().iterator();
//		while(it.hasNext()){
//			Map.Entry<Week, String> obj = (Entry<Week, String>) it.next();
//			weekHistogram.put(obj.getKey().toString(), new HistogramCluster(obj.getValue()));
//		}
//		
//		Map<String,String> month = this.lookup.getTweetsMonthsIndex(startDate, endDate);
//		it = month.entrySet().iterator();
//		while(it.hasNext()){
//			Map.Entry<String, String> obj = (Entry<String, String>) it.next();
//			monthHistogram.put(obj.getKey(), new HistogramCluster(obj.getValue()));
//		}
		
	}
	
	/**
	 * Given the maxlat, maxlon, minlat, and minlon this method create equal width size bucket of 
	 * Histogram. 
	 * @param max
	 * @param min
	 * @return list of partitions each is considered as bucket in histogram. 
	 * @throws IOException
	 */
	private List<Bucket> creatHistogramBuckets(Point max,Point min) throws IOException{
		List<Bucket> result = new ArrayList<Bucket>();
		double lonBucketSize = Math.abs((max.getLon()-min.getLon())/100);
		double latBucketSize =  Math.abs((max.getLat()-min.getLon())/100);
		Bucket part;
		//variables 
		double prelat = min.getLat();
		double preLon = min.getLon();
		double maxlat;
		double maxlon;
		int inc  =0;
		for(int x =0 ; x < 100 ; x++){
			maxlat = prelat + latBucketSize;
			for(int y =0; y < 100; y++){
				part = new Bucket();
				maxlon = preLon + lonBucketSize;
				//new mbr 
				MBR mbr = new MBR(new Point(maxlat,maxlon), new Point(prelat, preLon));
				part.setArea(mbr);
				part.setId(inc++);
				result.add(part);
				//change the prelon to the next value.
				preLon = maxlon;
			}
			prelat = maxlat;
			preLon = min.getLon();
		}
		return result;
	}
	
	
	private void initHistogramo(queryLevel level) throws ParseException{
		if(level.equals(queryLevel.Day)){
			initDayHistogram();
		}else if(level.equals(queryLevel.Week)){
			
		}else{
			
		}
	}
	
	
	private void initDayHistogram() throws ParseException{
		List<Bucket> temp;
		String day;
		long dayVolume = 0;
		Iterator it = dayHistogram.entrySet().iterator();
		while(it.hasNext()){
			temp = histogramBackets;
			Map.Entry<String, HistogramCluster> obj = (Entry<String,HistogramCluster>) it.next();
			day = obj.getKey();
			dayVolume = obj.getValue().getHistogramVolume();
			// invoke method to fill the histogram with persent of the cardinality.
			for(Bucket part: temp){
				double cardinality = getDayLevelCardinality(day, day, part.getArea()); 
				double persent = (cardinality/dayVolume);
//				System.out.println("Cardinality: "+cardinality+" persent :"+persent);
				part.setPersent(persent);
			}
			//put the new histogram with values 
			initHistogram.put(day, temp);
		}
	}
	
	private void initWeekHistogram() throws ParseException{
		List<Bucket> temp;
		String day;
		long dayVolume = 0;
		Iterator it = weekHistogram.entrySet().iterator();
		while(it.hasNext()){
			temp = histogramBackets;
			Map.Entry<String, HistogramCluster> obj = (Entry<String,HistogramCluster>) it.next();
			day = obj.getKey();
			dayVolume = obj.getValue().getHistogramVolume();
			// invoke method to fill the histogram with persent of the cardinality.
			for(Bucket part: temp){
				double cardinality = getWeekLevelCardinality(day, day, part.getArea()); 
				double persent = (cardinality/dayVolume);
				part.setPersent(persent);
			}
			//put the new histogram with values 
			initHistogram.put(day, temp);
		}
	}
	
	private void initMonthHistogram() throws ParseException{
		List<Bucket> temp;
		String day;
		long dayVolume = 0;
		Iterator it = monthHistogram.entrySet().iterator();
		while(it.hasNext()){
			temp = histogramBackets;
			Map.Entry<String, HistogramCluster> obj = (Entry<String,HistogramCluster>) it.next();
			day = obj.getKey();
			dayVolume = obj.getValue().getHistogramVolume();
			// invoke method to fill the histogram with persent of the cardinality.
			for(Bucket part: temp){
				double cardinality = getMonthLevelCardinality(day, day, part.getArea()); 
				double persent = (cardinality/dayVolume);
				part.setPersent(persent);
			}
			//put the new histogram with values 
			initHistogram.put(day, temp);
		}
	}
	
	
	private void matchDays(){
		List<String> keys = new ArrayList<String>();
		Iterator it = initHistogram.entrySet().iterator();
		//Add all the days,week,Month "keys" in temporary list 
		while(it.hasNext()){
			Map.Entry<String, List<Bucket>> obj = (Entry<String, List<Bucket>>) it.next();
			keys.add(obj.getKey());
		}
		
		for(String key: keys){
			if (initHistogram.containsKey(key)) {
				List<Bucket> histogram = initHistogram.get(key);
				 getSimilarHistogram(key, histogram);
			}
		}
	}
	
	private List<String> getSimilarHistogram(String timeSegment,List<Bucket> histogram){
		List<String> result = new ArrayList<String>();
		boolean samehistogramFlag = false;
		//Sort the histogram
		Collections.sort(histogram);
		Iterator it = initHistogram.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, List<Bucket>> obj = (Entry<String, List<Bucket>>) it.next();
			if(!timeSegment.equals(obj.getKey())){
				List<Bucket> temp = obj.getValue();
				Collections.sort(temp);
				samehistogramFlag = true;
				for(int i =0 ; i < temp.size(); i++){
					System.out.println("histogramID: "+histogram.get(i).getId() +" tempId:"+ temp.get(i).getId());
					if (histogram.get(i).getId() == temp.get(i).getId()) {
						if (histogram.get(i).getPersent() != temp.get(i)
								.getPersent()) {
							samehistogramFlag = false;
						}
					}
				}
				if(samehistogramFlag){
					result.add(obj.getKey());
				}
			}
		}
		//Delete the matched segments 
		deleteMatchedTemporalSegments(timeSegment, result);
		return result;
		
	}
	
	
	private void deleteMatchedTemporalSegments(String temporalSegment, List<String> matches){
		if (matches.size() != 0) {
			//add the day to the cluster then remove it from the list. 
			int clusterId = dayClusters.size()+1;
			dayClusters.add(new TemporalClusterTable(clusterId, initHistogram.get(temporalSegment)));
			for (String temporal : matches) {
				dayLookup.add(new TemporalLookupTable(temporal, dayHistogram.get(temporal).getHistogramVolume(), clusterId));
				initHistogram.remove(temporal);
			}
			initHistogram.remove(temporalSegment);
		}
	}
	
	
	
	
	public queryLevel getQueryPlan(String startDay,String endDay,MBR queryMBR) throws ParseException{
		queryLevel queryFrom = queryLevel.Day;
		double minPlan = Double.MAX_VALUE;
		for (queryLevel q : queryLevel.values()) {
			long plancost = this.getQueryCost(startDay, endDay, q, queryMBR);
			System.out.println("Histogram Statistics: \n" + q.toString()
					+ "\tCardinality Cost: " + plancost);
			if (plancost <= minPlan && plancost != -1) {
				minPlan = plancost;
				queryFrom = q;
			}
		}
		return queryFrom;
		
	}
	
	
	
	
	
	private long getQueryCost(String startDay, String endDay, queryLevel q,
			MBR queryMBR) throws ParseException {
		if (q.equals(queryLevel.Day)) {
			return this.getDayLevelCardinality(startDay, endDay, queryMBR);
		} else if (q.equals(queryLevel.Week)) {
			return this.getWeekLevelCardinality(startDay, endDay, queryMBR);
		} else if (q.equals(queryLevel.Month)) {
			return this.getMonthLevelCardinality(startDay, endDay, queryMBR);
		}
		return 0;
	}
	
	
	private long getDayLevelCardinality(String startDay,String endDay,MBR queryMBR) throws ParseException{
		//clusterCalculator<clusterid,numberofdays>
		//temporalRange<day,directorypath>
		long result = -1;
		HashMap<Integer,Integer> clusterCalculator = new HashMap<Integer, Integer>();
		Map<String,String> temporalRange = this.lookup.getTweetsDayIndex(startDay, endDay);
		Iterator it = temporalRange.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String,String> day = (Entry<String, String>) it.next();
			HistogramCluster histogram = dayHistogram.get(day.getKey());
			try{
			result += histogram.getCardinality(queryMBR);
			}catch(NullPointerException e){
				
			}
		}
		return result;
	}
	
	private long getMonthLevelCardinality(String startDay,String endDay,MBR queryMBR) throws ParseException{
		//clusterCalculator<clusterid,numberofdays>
		//temporalRange<day,directorypath>
		long result = -1;
		HashMap<Integer,Integer> clusterCalculator = new HashMap<Integer, Integer>();
		Map<String,String> temporalRange = this.lookup.getTweetsMonthsIndex(startDay, endDay);
		Iterator it = temporalRange.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String,String> m = (Entry<String, String>) it.next();
			HistogramCluster histogram = monthHistogram.get(m.getKey());
			try{
			result += histogram.getCardinality(queryMBR);
			}catch(NullPointerException e){
				
			}
		}
		return result;
	}
	
	
	private long getWeekLevelCardinality(String startDay,String endDay,MBR queryMBR) throws ParseException{
		//clusterCalculator<clusterid,numberofdays>
		//temporalRange<day,directorypath>
		long result = -1;
		HashMap<Integer,Integer> clusterCalculator = new HashMap<Integer, Integer>();
		Map<Week,String> temporalRange = this.lookup.getTweetsWeekIndex(startDay, endDay);
		Iterator it = temporalRange.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Week,String> week = (Entry<Week, String>) it.next();
			HistogramCluster histogram = weekHistogram.get(week.getKey().toString());
			try{
				result += histogram.getCardinality(queryMBR);
			}catch(NullPointerException e){
				System.out.println(week.getValue());
			}
		}
		return result;
	}
	
	

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
	
		//MBR mbr = new MBR(new Point(40.694961541009995,118.07045041992582),new Point(38.98904106170265,114.92561399414794) );
		//40.06978032458496 21.29929525830078, 40.06978032458496 21.56846029736328, 39.55651280749512 21.56846029736328, 39.55651280749512 21.29929525830078, 40.06978032458496 21.29929525830078
		MBR mbr = new MBR(new Point(21.56846029736328, 40.06978032458496), new Point(21.29929525830078, 39.55651280749512));

		
		
		MemoryHistogram planner2 = new MemoryHistogram();
		
//		System.err.println("Done");
//		System.out.println("Num of day histogram: "+planner2.dayHistogram.size());
//		System.out.println("Num of week histogram: "+planner2.weekHistogram.size());
//		System.out.println("Num of month histogram: "+planner2.monthHistogram.size());
//		queryLevel result = planner2.getQueryPlan("2014-05-01", "2014-05-10", mbr);
//		 System.out.print(result);

	}
	
	
	
	

}
