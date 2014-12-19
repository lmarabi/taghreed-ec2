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

	public TraditionalMultiHistogram(Lookup lookup) throws IOException {
		// init in memory histograms
		this.lookup = lookup;
		this.dayHistogram = new HashMap<String, HistogramCluster>();
		this.weekHistogram = new HashMap<String, HistogramCluster>();
		this.monthHistogram = new HashMap<String, HistogramCluster>();
		// Offline processing
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
		
		System.out.println();
		
	}
	
	public long getQueryPlan(String startDay,String endDay,MBR queryMBR) throws ParseException{
		return getDayLevelCardinality(startDay, endDay, queryMBR);
		
	}
	
	
	
	private long getDayLevelCardinality(String startDay,String endDay,MBR queryMBR) throws ParseException{
		//clusterCalculator<clusterid,numberofdays>
		//temporalRange<day,directorypath>
		long result = 0;
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
	
	

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
		ServerRequest server = new ServerRequest();
		MBR mbr = new MBR(new Point(40.694961541009995,118.07045041992582),new Point(38.98904106170265,114.92561399414794) );
		server.setMBR(mbr);
		server.setIndex(queryIndex.rtree);
		TraditionalMultiHistogram planner2 = new TraditionalMultiHistogram(server.getLookup());
		planner2.OfflinePhase();
		System.err.println("Done");
		System.out.println("Num of day histogram: "+planner2.dayHistogram.size());
		System.out.println("Num of week histogram: "+planner2.weekHistogram.size());
		System.out.println("Num of month histogram: "+planner2.monthHistogram.size());
		long result = planner2.getQueryPlan("2014-05-01", "2014-05-01", server.getMbr());
		 System.out.print(result);

	}

}