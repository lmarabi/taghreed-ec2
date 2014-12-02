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
	
	public QueryPlanner() throws IOException, ParseException {
		this.LonDomain = 360;
		this.LatDomain = 180;
		this.dayHistogram = new HistogramCell[LonDomain][LatDomain];
		this.weekHistogram = new HistogramCell[LonDomain][LatDomain];
		this.monthHistogram = new HistogramCell[LonDomain][LatDomain];
		for (queryLevel q : queryLevel.values()) {
			this.readHistogramFromDisk(q);
		}
	}

	/**
	 * This method will read the stored histogram from the disk
	 * 
	 * @throws IOException
	 * @throws ParseException
	 */
	public void readHistogramFromDisk(queryLevel q) throws IOException, ParseException {
		BufferedReader reader = new BufferedReader(new FileReader(
				System.getProperty("user.dir") + "/_Histogram_"
						+ q.toString() + ".txt"));
		ServerRequest req = new ServerRequest();
		req.setIndex(queryIndex.rtree);
		req.setType(queryType.tweet);
		String line;
		int lon, lat;
		while ((line = reader.readLine()) != null) {
			String[] token = line.split(",");
			lon = Integer.parseInt(token[0]);
			lat = Integer.parseInt(token[1]);
			HistogramCell temp = new HistogramCell(token[2], req.getLookup());
			if(q.equals(queryLevel.Day)){
				this.dayHistogram[lon][lat] = temp;
			}else if(q.equals(queryLevel.Week)){
				this.weekHistogram[lon][lat] = temp;
			}else{
				this.monthHistogram[lon][lat] = temp;
			}
			
		}
		reader.close();
	}
	
	private double getQueryCost(String startDay, String endDay, queryLevel queryLevel , MBR mbr) throws ParseException{
		this.startDay = startDay;
		this.endDay = endDay;
		this.level = queryLevel;
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
		int minlon = (int) Math.ceil(mbr.getMin().getLon()+180)- 1;
		minlon = (minlon < 0 ) ? 0 : minlon;
		int maxlon = (int) Math.ceil(mbr.getMax().getLon()+180) +1;
		maxlon = (maxlon > 360) ? 360 : maxlon;
		int minlat = (int) Math.ceil(mbr.getMin().getLat()+90) -1;
		minlat = (minlat < 0 ) ? 0 : minlat;
		int maxlat = (int) Math.ceil(mbr.getMax().getLat()+90) +1;
		maxlat = (maxlat > 180) ? 180 : maxlat;
		for (int i = minlon; i < maxlon; i++) {
			for (int j = minlat; j < maxlat; j++) {
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
		int minlon = (int) Math.ceil(mbr.getMin().getLon()+180)- 1;
		minlon = (minlon < 0 ) ? 0 : minlon;
		int maxlon = (int) Math.ceil(mbr.getMax().getLon()+180) +1;
		maxlon = (maxlon > 360) ? 360 : maxlon;
		int minlat = (int) Math.ceil(mbr.getMin().getLat()+90) -1;
		minlat = (minlat < 0 ) ? 0 : minlat;
		int maxlat = (int) Math.ceil(mbr.getMax().getLat()+90) +1;
		maxlat = (maxlat > 180) ? 180 : maxlat;
		for (int i = minlon; i < maxlon; i++) {
			for (int j = minlat; j < maxlat; j++) {
				try {
					if (mbr.Intersect(monthHistogram[i][j].mbr)) {
//						System.out.println("cellid:" + i + "-" + j + "  "
//								+ monthHistogram[i][j].getMbr().toString());
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
		int minlon = (int) Math.ceil(mbr.getMin().getLon()+180)- 1;
		minlon = (minlon < 0 ) ? 0 : minlon;
		int maxlon = (int) Math.ceil(mbr.getMax().getLon()+180) +1;
		maxlon = (maxlon > 360) ? 360 : maxlon;
		int minlat = (int) Math.ceil(mbr.getMin().getLat()+90) -1;
		minlat = (minlat < 0 ) ? 0 : minlat;
		int maxlat = (int) Math.ceil(mbr.getMax().getLat()+90) +1;
		maxlat = (maxlat > 180) ? 180 : maxlat;
		for (int i = minlon; i < maxlon; i++) {
			for (int j = minlat; j < maxlat; j++) {
				try {
					if (mbr.Intersect(dayHistogram[i][j].mbr)) {
//						System.out.println("cellid:" + i + "-" + j + "  "
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
	
	/**
	 * This method gives the best query plan 
	 * @param startDay
	 * @param endDay
	 * @param mbr
	 * @return
	 * @throws ParseException
	 */
	public queryLevel getQueryPlan(String startDay, String endDay, MBR mbr) throws ParseException{
		queryLevel queryFrom = queryLevel.Day;
		double minPlan = Double.MAX_VALUE;
		for(queryLevel q : queryLevel.values()){
			double plancost = this.getQueryCost(startDay, endDay, q, mbr);
			System.out.println("Histogram Statistics: \n"+q.toString()+"\tCardinality Cost: "+plancost);
			if(plancost <= minPlan && plancost != 0){
				minPlan = plancost;
				queryFrom = q;
			}
		}
		return queryFrom;
	}
	
//	private double getFastIntersectCell(MBR mbr) throws ParseException{
//		double cost = 0;
//		int cell = 0;
//		int minlon = (int) Math.ceil(mbr.getMin().getLon()+180)- 1;
//		minlon = (minlon < 0 ) ? 0 : minlon;
//		int maxlon = (int) Math.ceil(mbr.getMax().getLon()+180) +1;
//		maxlon = (maxlon > 360) ? 360 : maxlon;
//		int minlat = (int) Math.ceil(mbr.getMin().getLat()+90) -1;
//		minlat = (minlat < 0 ) ? 0 : minlat;
//		int maxlat = (int) Math.ceil(mbr.getMax().getLat()+90) +1;
//		maxlat = (maxlat > 180) ? 180 : maxlat;
//		for (int i = minlon; i < maxlon; i++) {
//			for (int j = minlat; j < maxlat; j++) {
//				try {
//					if (mbr.Intersect(dayHistogram[i][j].mbr)) {
////						System.out.println("cellid:" + i + "-" + j + "  "
////								+ dayHistogram[i][j].getMbr().toString());
//						cell++;
//						cost += dayHistogram[i][j].getEsitimatedCostDayHistogram(startDay,
//								endDay);
//					}
//				} catch (NullPointerException e) {
//
//				}
//			}
//		}
//		System.out.println("Cost:"+cost+" intersected Cell:"+cell);
//		return cost;
//	}

	public static void main(String[] args) throws IOException, ParseException {

//		MBR mbr = new MBR(new Point(90, 180), new Point(-90, -180));

		MBR mbr = new MBR(new Point(40.694961541009995,118.07045041992582),new Point(38.98904106170265,114.92561399414794) );
//		 MBR mbr = new MBR(new Point(46.68419444691358,-73.67156982421847),new
//		 Point(39.61732577224177,-76.47308349609341));

		QueryPlanner planner = new QueryPlanner();
		for (queryLevel q : queryLevel.values()) {

//			if (q.equals(queryLevel.Month) || q.equals(queryLevel.Day))
//				continue;
			long starttime = System.currentTimeMillis();
			double estimatedCardinality = planner.getQueryCost("2014-10-01", "2014-10-30",
					q, mbr);
			long endtime = System.currentTimeMillis();
			System.out.println("estimated cardinality "+q.toString()+" :"+ estimatedCardinality);
			System.out.println("Time in Milliseconds: " + (endtime - starttime));
			System.out.println("*************************");

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
