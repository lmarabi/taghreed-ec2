package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.gistic.taghreed.Commons;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.Week;
import org.gistic.taghreed.diskBaseQuery.query.Lookup;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryIndex;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;

public class QueryPlanner2 {
	String startDay;
	String endDay;
	GridCell worldCell;
	HashMap<Integer,HistogramCluster> dayHistogram;
	HashMap<Integer,HistogramCluster> weekHistogram;
	HashMap<Integer,HistogramCluster> monthHistogram;
	List<DayCardinality> dayCardinality;
	List<DayCardinality> weekCardinality;
	List<DayCardinality> montCardinality;
	queryLevel level;
	Lookup lookup;
	double confidenceThreshold = 0.75;

	public QueryPlanner2(Lookup lookup) throws IOException {
		// init in memory histograms
		this.lookup = lookup;
		this.dayHistogram = new HashMap<Integer, HistogramCluster>();
		this.weekHistogram = new HashMap<Integer, HistogramCluster>();
		this.monthHistogram = new HashMap<Integer, HistogramCluster>();
		this.dayCardinality = new ArrayList<DayCardinality>();
		this.weekCardinality = new ArrayList<DayCardinality>();
		this.montCardinality = new ArrayList<DayCardinality>();
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
		this.worldCell = new GridCell(mbr, lookup);
		BufferedReader reader = new BufferedReader(new FileReader(new File(
				System.getProperty("user.dir") + "/_inputDataStatistics_.txt")));
		String line; 
		while((line = reader.readLine())!= null ){
			System.out.println(line);
			String[]token = line.split("_");
			this.worldCell.add(token[0], Long.parseLong(token[1]));
		}
		
		//Create Histogram for days
		this.worldCell.initCluster(confidenceThreshold);
		List<Cluster> clusters = this.worldCell.getCluster();
		int clusterid = 0;
		for(Cluster c : clusters){
			// build the histogram of the median day
			String median = c.getMedianDay();
			String path = Commons.queryRtreeIndex+"tweets/Day/index."+median;
			System.out.println(path);
			this.dayHistogram.put(clusterid,new HistogramCluster(path));
			//add the cluster id to dayCardinality list
			List<DayCardinality> clusterDays = c.getDays();
			for(DayCardinality day : clusterDays){
				DayCardinality temp = new DayCardinality(day.getDay(),day.getCardinality());
				temp.setClusterId(clusterid);
				this.dayCardinality.add(temp);
			}
			clusterid++;
		}
		
		this.worldCell = new GridCell(mbr, lookup);
		//crate histogram for week
		List<Week> weeks = this.lookup.getWeekDatesTweet();
		int cardinality;
		for(Week week : weeks){
			cardinality = 0;
			for(DayCardinality day : this.dayCardinality){
				if(week.isDayIntheWeek(day.getDay())){
					cardinality += day.getCardinality();
				}
			}
			//this.weekCardinality.add(new DayCardinality(week.toString(), cardinality));
			this.worldCell.add(week.toString(), cardinality);
		}
		//init cluster
		this.worldCell.initCluster(confidenceThreshold);
		clusters = this.worldCell.getCluster();
		clusterid = 0;
		for (Cluster c : clusters) {
			// build the histogram of the median day
			String median = c.getMedianDay();
			String path = Commons.queryRtreeIndex + "tweets/Week/index."
					+ median;
			System.out.println(path);
			this.weekHistogram.put(clusterid, new HistogramCluster(path));
			// add the cluster id to dayCardinality list
			List<DayCardinality> clusteritmes = c.getDays();
			for (DayCardinality item : clusteritmes) {
				DayCardinality temp = new DayCardinality(item.getDay(),
						item.getCardinality());
				temp.setClusterId(clusterid);
				this.weekCardinality.add(temp);
			}
			clusterid++;

		}
		
		//create histogram for the months
//		this.worldCell = new GridCell(mbr, lookup);
//		List<String> months = this.lookup.getMonthDatesTweet();
//		for(String month : months){
//			cardinality = 0;
//			for(DayCardinality day : this.dayCardinality){
//				if(week.isDayIntheWeek(day.getDay())){
//					cardinality += day.getCardinality();
//				}
//			}
//			//this.weekCardinality.add(new DayCardinality(week.toString(), cardinality));
//			this.worldCell.add(week.toString(), cardinality);
//		}
//		//init cluster
//		this.worldCell.initCluster(confidenceThreshold);
//		clusters = this.worldCell.getCluster();
//		clusterid = 0;
//		for (Cluster c : clusters) {
//			// build the histogram of the median day
//			String median = c.getMedianDay();
//			String path = Commons.queryRtreeIndex + "tweets/Week/index."
//					+ median;
//			System.out.println(path);
//			this.weekHistogram.put(clusterid, new HistogramCluster(path));
//			// add the cluster id to dayCardinality list
//			List<DayCardinality> clusteritmes = c.getDays();
//			for (DayCardinality item : clusteritmes) {
//				DayCardinality temp = new DayCardinality(item.getDay(),
//						item.getCardinality());
//				temp.setClusterId(clusterid);
//				this.weekCardinality.add(temp);
//			}
//			clusterid++;
//
//		}
		
	}

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
		ServerRequest server = new ServerRequest();
		server.setIndex(queryIndex.rtree);
		QueryPlanner2 planner2 = new QueryPlanner2(server.getLookup());
		planner2.OfflinePhase();
		System.err.println("Done");
		System.out.println("Num of day histogram: "+planner2.dayHistogram.size());
		System.out.println("Num of week histogram: "+planner2.weekHistogram.size());
		

	}

}
