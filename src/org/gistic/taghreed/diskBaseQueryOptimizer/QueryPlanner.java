package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.diskBaseQuery.query.Lookup;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryIndex;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryType;
import org.joda.time.Years;

public class QueryPlanner {
	String startDay;
	String endDay;
	int LonDomain;
	int LatDomain;
	HistogramCell[][] dayHistogram;
	HistogramCell[][] weekHistogram;
	HistogramCell[][] monthHistogram;
	queryLevel level;

	public QueryPlanner(String startDay, String endDay, queryLevel queryLevel) {
		this.startDay = startDay;
		this.endDay = endDay;
		this.LonDomain = 360;
		this.LatDomain = 180;
		this.dayHistogram = new HistogramCell[LonDomain][LatDomain];
		this.weekHistogram = new HistogramCell[LonDomain][LatDomain];
		this.monthHistogram = new HistogramCell[LonDomain][LatDomain];
		this.level = queryLevel;
	}

	/**
	 * This method will read the stored histogram from the disk
	 * 
	 * @throws IOException
	 * @throws ParseException
	 */
	public void readHistogramFromDisk() throws IOException, ParseException {
		BufferedReader reader = new BufferedReader(new FileReader(
				System.getProperty("user.dir") + "/_Histogram_"
						+ this.level.toString() + ".txt"));
		ServerRequest req = new ServerRequest(1);
		req.setIndex(queryIndex.rtree);
		req.setType(queryType.tweet);
		String line;
		int lon, lat;
		while ((line = reader.readLine()) != null) {
			String[] token = line.split(",");
			lon = Integer.parseInt(token[0]);
			lat = Integer.parseInt(token[1]);
			HistogramCell temp = new HistogramCell(token[2], req.getLookup());
			if(this.level.equals(queryLevel.Day)){
				this.dayHistogram[lon][lat] = temp;
			}else if(this.level.equals(queryLevel.Week)){
				this.weekHistogram[lon][lat] = temp;
			}else{
				this.monthHistogram[lon][lat] = temp;
			}
			
		}
		reader.close();
	}
	
	public double getIntersectCellHistogram(MBR mbr) throws ParseException{
		if(this.level.equals(queryLevel.Day)){
			return getIntersectCellsDayHistogram(mbr);
		}else if(this.level.equals(queryLevel.Week)){
			return getIntersectCellsWeekHistogram(mbr);
		}else{
			return getIntersectCellsMonthHistogram(mbr);
		}
	}
	
	private double getIntersectCellsWeekHistogram(MBR mbr) throws ParseException {
		double cost = 0;
		int cell = 0;
		for (int i = 0; i < this.LonDomain; i++) {
			for (int j = 0; j < this.LatDomain; j++) {
				try {
					if (mbr.Intersect(weekHistogram[i][j].mbr)) {
//						System.out.println("cellid:" + j + "-" + i + "  "
//								+ dayHistogram[i][j].getMbr().toString());
						cell++;
						cost += weekHistogram[i][j].getEsitimatedCostWeekHistogram(startDay,
								endDay);
					}
				} catch (NullPointerException e) {

				}
			}
		}
		System.out.println("Cost:"+cost+" intersected Cell:"+cell);
		return cost;
	}
	
	private double getIntersectCellsMonthHistogram(MBR mbr) throws ParseException {
		double cost = 0;
		int cell = 0;
		for (int i = 0; i < this.LonDomain; i++) {
			for (int j = 0; j < this.LatDomain; j++) {
				try {
					if (mbr.Intersect(monthHistogram[i][j].mbr)) {
//						System.out.println("cellid:" + j + "-" + i + "  "
//								+ dayHistogram[i][j].getMbr().toString());
						cell++;
						cost += monthHistogram[i][j].getEsitimatedCostMonthHistogram(startDay,
								endDay);
					}
				} catch (NullPointerException e) {

				}
			}
		}
		System.out.println("Cost:"+cost+" intersected Cell:"+cell);
		return cost;
	}

	
	private double getIntersectCellsDayHistogram(MBR mbr) throws ParseException {
		double cost = 0;
		int cell = 0;
		for (int i = 0; i < this.LonDomain; i++) {
			for (int j = 0; j < this.LatDomain; j++) {
				try {
					if (mbr.Intersect(dayHistogram[i][j].mbr)) {
//						System.out.println("cellid:" + j + "-" + i + "  "
//								+ dayHistogram[i][j].getMbr().toString());
						cell++;
						cost += dayHistogram[i][j].getEsitimatedCostDayHistogram(startDay,
								endDay);
					}
				} catch (NullPointerException e) {

				}
			}
		}
		System.out.println("Cost:"+cost+" intersected Cell:"+cell);
		return cost;
	}

	public static void main(String[] args) throws IOException, ParseException {

//		MBR mbr = new MBR(new Point(90, 180), new Point(-90, -180));

		 MBR mbr = new MBR(new Point(46.68419444691358,-73.67156982421847),new
		 Point(39.61732577224177,-76.47308349609341));

		for (queryLevel q : queryLevel.values()) {

//			if (q.equals(queryLevel.Day) || q.equals(queryLevel.Month))
//				continue;

			QueryPlanner planner = new QueryPlanner("2014-01-01", "2014-05-30",
					q);
			System.out.println("Reading histogram " + q.toString());
			planner.readHistogramFromDisk();
			long starttime = System.currentTimeMillis();
			double estimatedCardinality = planner.getIntersectCellHistogram(mbr);
			long endtime = System.currentTimeMillis();
			System.out.println("estimated cardinality "+q.toString()+" :"+ estimatedCardinality);
			System.out.println("Time in Milliseconds: " + (endtime - starttime));

		}
		
		 
//		String startDay = "2014-05-10";
//		String endDay = "2014-10-20";
//		Date start = Lookup.dateFormat.parse(startDay);
//        Date end = Lookup.dateFormat.parse(endDay);
//        Calendar c = Calendar.getInstance();
//        c.setTime(start);
//        while(start.getYear() != end.getYear() || start.getMonth() != end.getMonth()){
//        	System.out.println(c.get(Calendar.YEAR)+"-"+(start.getMonth()+1));
//        	c.add(Calendar.MONTH, 1);
//        	start = c.getTime();
//        	
//        }
//        System.out.println(c.get(Calendar.YEAR)+"-"+(start.getMonth()+1));
		
	}

}
