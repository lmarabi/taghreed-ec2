package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.gistic.taghreed.Commons;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.Week;
import org.gistic.taghreed.diskBaseQuery.query.Lookup;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryIndex;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;

public class TraditionalMultiHistogram {
	String startDay;
	String endDay;
	GridCell worldCell;
	HashMap<String,HistogramCluster> dayHistogram;
	HashMap<String,HistogramCluster> weekHistogram;
	HashMap<String,HistogramCluster> monthHistogram;
	queryLevel level;
	Lookup lookup;
	double confidenceThreshold = 0.75;

	public TraditionalMultiHistogram() throws IOException, ParseException {
		// init in memory histograms
		ServerRequest server = new ServerRequest();
		server.setIndex(queryIndex.rtree);
		this.lookup = server.getLookup();
		this.dayHistogram = new HashMap<String, HistogramCluster>();
		this.weekHistogram = new HashMap<String, HistogramCluster>();
		this.monthHistogram = new HashMap<String, HistogramCluster>();
		// Offline processing
		 OfflinePhase();
	}

	/**
	 * This method is offline phase of to prepare the clusters of the histogram.
	 * 
	 * @throws IOException
	 * @throws ParseException 
	 */
	private void OfflinePhase() throws IOException, ParseException {
		MBR mbr = new MBR(new Point(90, 180), new Point(-90, -180));
		String startDate = "2000-01-01";
		String endDate = "3000-01-01";
		Map<String,String> days = this.lookup.getTweetsDayIndex(startDate, endDate);
		Iterator it = days.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, String> obj = (Entry<String, String>) it.next();
			dayHistogram.put(obj.getKey(), new HistogramCluster(obj.getValue()));
		}
		
		Map<Week,String> week = this.lookup.getAllTweetsWeekIndex(startDate, endDate);
		it = week.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Week, String> obj = (Entry<Week, String>) it.next();
			weekHistogram.put(obj.getKey().toString(), new HistogramCluster(obj.getValue()));
		}
		
		Map<String,String> month = this.lookup.getTweetsMonthsIndex(startDate, endDate);
		it = month.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, String> obj = (Entry<String, String>) it.next();
			monthHistogram.put(obj.getKey(), new HistogramCluster(obj.getValue()));
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
			result += histogram.getCardinality(queryMBR);
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
			result += histogram.getCardinality(queryMBR);
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
			result += histogram.getCardinality(queryMBR);
		}
		return result;
	}
	
	

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
	
		//MBR mbr = new MBR(new Point(40.694961541009995,118.07045041992582),new Point(38.98904106170265,114.92561399414794) );
		//40.06978032458496 21.29929525830078, 40.06978032458496 21.56846029736328, 39.55651280749512 21.56846029736328, 39.55651280749512 21.29929525830078, 40.06978032458496 21.29929525830078
		MBR mbr = new MBR(new Point(21.56846029736328, 40.06978032458496), new Point(21.29929525830078, 39.55651280749512));

		
		
		TraditionalMultiHistogram planner2 = new TraditionalMultiHistogram();
		
		System.err.println("Done");
		System.out.println("Num of day histogram: "+planner2.dayHistogram.size());
		System.out.println("Num of week histogram: "+planner2.weekHistogram.size());
		System.out.println("Num of month histogram: "+planner2.monthHistogram.size());
		queryLevel result = planner2.getQueryPlan("2014-05-01", "2014-05-10", mbr);
		 System.out.print(result);

	}

}