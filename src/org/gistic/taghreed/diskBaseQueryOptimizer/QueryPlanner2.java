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

public class QueryPlanner2 {
	GridCell worldCell;
	HashMap<Integer, HistogramCluster> dayHistogram;
	HashMap<Integer, HistogramCluster> weekHistogram;
	HashMap<Integer, HistogramCluster> monthHistogram;
	List<DayCardinality> dayArrayList;
	List<DayCardinality> weekArrayList;
	List<DayCardinality> monthArrayList;
	queryLevel level;
	Lookup lookup;
	double confidenceThreshold = 0.85;

	public QueryPlanner2() throws FileNotFoundException, IOException, ParseException {
		// init in memory histograms
		ServerRequest server = new ServerRequest();
		server.setIndex(queryIndex.rtree);
		this.lookup = server.getLookup();
		this.dayHistogram = new HashMap<Integer, HistogramCluster>();
		this.weekHistogram = new HashMap<Integer, HistogramCluster>();
		this.monthHistogram = new HashMap<Integer, HistogramCluster>();
		this.dayArrayList = new ArrayList<DayCardinality>();
		this.weekArrayList = new ArrayList<DayCardinality>();
		this.monthArrayList = new ArrayList<DayCardinality>();
		this.OfflinePhase();
		//
		System.out.println("Done");
		System.out.println("Num of day histogram: "
				+ this.dayHistogram.size());
		System.out.println("Num of week histogram: "
				+ this.weekHistogram.size());
		System.out.println("Num of month histogram: "
				+ this.monthHistogram.size());
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
		while ((line = reader.readLine()) != null) {
			System.out.println(line);
			String[] token = line.split("_");
			this.worldCell.add(token[0], Long.parseLong(token[1]));
		}

		// Create Histogram for days
		this.worldCell.initCluster(confidenceThreshold);
		List<Cluster> clusters = this.worldCell.getCluster();
		int clusterid = 0;
		for (Cluster c : clusters) {
			// build the histogram of the median day
			String median = c.getMedianDay();
			String path = Commons.queryRtreeIndex + "tweets/Day/index."
					+ median;
			System.out.println(path);
			this.dayHistogram.put(clusterid, new HistogramCluster(path));
			// add the cluster id to dayCardinality list
			List<DayCardinality> clusterDays = c.getDays();
			for (DayCardinality day : clusterDays) {
				DayCardinality temp = new DayCardinality(day.getDay(),
						day.getCardinality());
				temp.setClusterId(clusterid);
				this.dayArrayList.add(temp);
			}
			clusterid++;
		}

		this.worldCell = new GridCell(mbr, lookup);
		// crate histogram for week
		List<Week> weeks = this.lookup.getWeekDatesTweet();
		int cardinality;
		for (Week week : weeks) {
			cardinality = 0;
			for (DayCardinality day : this.dayArrayList) {
				if (week.isDayIntheWeek(day.getDay())) {
					cardinality += day.getCardinality();
				}
			}
			this.worldCell.add(week.toString(), cardinality);
		}
		// init cluster
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
				this.weekArrayList.add(temp);
			}
			clusterid++;

		}

		// create histogram for the months
		this.worldCell = new GridCell(mbr, lookup);
		List<String> months = this.lookup.getTweetsMonth("2000-01-01",
				"3000-01-01");
		cardinality = 0;
		for (String m : months) {
			cardinality = 0;
			for (DayCardinality day : this.dayArrayList) {
				if (m.equals(day.getDay().substring(0, 7))) {
					cardinality += day.getCardinality();
				}
			}
			if (cardinality != 0) {
				this.worldCell.add(m, cardinality);
			}
		}
		// init cluster
		this.worldCell.initCluster(confidenceThreshold);
		clusters = this.worldCell.getCluster();
		clusterid = 0;
		for (Cluster c : clusters) {
			// build the histogram of the median day
			String median = c.getMedianDay();
			String path = Commons.queryRtreeIndex + "tweets/Month/index."
					+ median;
			System.out.println(path);
			this.monthHistogram.put(clusterid, new HistogramCluster(path));
			// add the cluster id to dayCardinality list
			List<DayCardinality> clusteritmes = c.getDays();
			for (DayCardinality item : clusteritmes) {
				DayCardinality temp = new DayCardinality(item.getDay(),
						item.getCardinality());
				temp.setClusterId(clusterid);
				this.monthArrayList.add(temp);
			}
			clusterid++;

		}

		System.out.println();

	}

	public queryLevel getQueryPlan(String startDay, String endDay, MBR queryMBR)
			throws ParseException {
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
			return this.getMonthkLevelCardinality(startDay, endDay, queryMBR);
		}
		return 0;
	}

	private long getMonthkLevelCardinality(String startDay, String endDay,
			MBR queryMBR) throws ParseException {
		// clusterCalculator<clusterid,numberofdays>
		// temporalRange<day,directorypath>

		HashMap<Integer, Integer> clusterCalculator = new HashMap<Integer, Integer>();
		Map<String, String> temporalRange = this.lookup.getTweetsMonthwithDir(
				startDay, endDay);
		Iterator it = temporalRange.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Week, String> day = (Entry<Week, String>) it.next();
			for (DayCardinality item : monthArrayList) {
				if (item.getDay().equals(day.getKey())) {
					if (clusterCalculator.containsKey(item.getClusterId())) {
						clusterCalculator.put(item.getClusterId(),
								clusterCalculator.get(item.getClusterId()) + 1);
					} else {
						clusterCalculator.put(item.getClusterId(), 1);
					}
				}
			}
		}

		// Calculate the actual cluster values
		long result = -1;
		it = clusterCalculator.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, Integer> obj = (Entry<Integer, Integer>) it
					.next();
			HistogramCluster histogram = monthHistogram.get(obj.getKey());
			result += (histogram.getCardinality(queryMBR) * obj.getValue());
		}
		return result;
	}

	private long getWeekLevelCardinality(String startDay, String endDay,
			MBR queryMBR) throws ParseException {
		// clusterCalculator<clusterid,numberofdays>
		// temporalRange<day,directorypath>

		HashMap<Integer, Integer> clusterCalculator = new HashMap<Integer, Integer>();
		Map<Week, String> temporalRange = this.lookup.getTweetsWeekIndex(
				startDay, endDay);
		Iterator it = temporalRange.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Week, String> day = (Entry<Week, String>) it.next();
			for (DayCardinality item : weekArrayList) {
				if (item.getDay().equals(day.getKey().toString())) {
					if (clusterCalculator.containsKey(item.getClusterId())) {
						clusterCalculator.put(item.getClusterId(),
								clusterCalculator.get(item.getClusterId()) + 1);
					} else {
						clusterCalculator.put(item.getClusterId(), 1);
					}
				}
			}
		}

		// Calculate the actual cluster values
		long result = -1;
		it = clusterCalculator.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, Integer> obj = (Entry<Integer, Integer>) it
					.next();
			HistogramCluster histogram = weekHistogram.get(obj.getKey());
			result += (histogram.getCardinality(queryMBR) * obj.getValue());
		}
		return result;
	}

	private long getDayLevelCardinality(String startDay, String endDay,
			MBR queryMBR) throws ParseException {
		// clusterCalculator<clusterid,numberofdays>
		// temporalRange<day,directorypath>

		HashMap<Integer, Integer> clusterCalculator = new HashMap<Integer, Integer>();
		Map<String, String> temporalRange = new HashMap<String, String>();
		temporalRange = lookup.getTweetsDayIndex(
				startDay, endDay);
		Iterator it = temporalRange.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> day = (Entry<String, String>) it.next();
			for (DayCardinality item : dayArrayList) {
				if (item.getDay().equals(day.getKey())) {
					if (clusterCalculator.containsKey(item.getClusterId())) {
						clusterCalculator.put(item.getClusterId(),
								clusterCalculator.get(item.getClusterId()) + 1);
					} else {
						clusterCalculator.put(item.getClusterId(), 1);
					}
				}
			}
		}

		// Calculate the actual cluster values
		long result = -1;
		it = clusterCalculator.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, Integer> obj = (Entry<Integer, Integer>) it
					.next();
			HistogramCluster histogram = dayHistogram.get(obj.getKey());
			result += (histogram.getCardinality(queryMBR) * obj.getValue());
		}
		return result;
	}

	public void printArray() {
		System.err.println("day\tClusterID");
		for (DayCardinality day : dayArrayList) {
			System.out.println(day.getDay() + "\t" + day.getClusterId());
		}
		System.err.println("week\tClusterID");
		for (DayCardinality day : weekArrayList) {
			System.out.println(day.getDay() + "\t" + day.getClusterId());
		}
		System.err.println("month\tClusterID");
		for (DayCardinality day : monthArrayList) {
			System.out.println(day.getDay() + "\t" + day.getClusterId());
		}
	}

	public static void main(String[] args) throws FileNotFoundException,
			IOException, ParseException {
		
		MBR mbr = new MBR(new Point(40.694961541009995, 118.07045041992582),
				new Point(38.98904106170265, 114.92561399414794));
		
		QueryPlanner2 planner2 = new QueryPlanner2();
		
		
		 planner2.printArray();
		queryLevel q = planner2.getQueryPlan("2014-05-01", "2014-05-10",
				mbr);
		System.out.print(q.toString());

	}

}
