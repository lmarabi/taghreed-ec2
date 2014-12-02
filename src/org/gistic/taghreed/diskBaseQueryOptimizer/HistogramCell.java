package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.collections.Week;
import org.gistic.taghreed.diskBaseQuery.query.Lookup;

public class HistogramCell {
	private static Lookup lookup;
	MBR mbr;
	private List<Cluster> cluster = new ArrayList<Cluster>();
	
	public HistogramCell(String parsString,Lookup lookup){
		String[] token = parsString.split(";");
		this.mbr = new MBR(token[0]);
		for(int i=1;i<token.length;i++){
				Cluster clustObj = new Cluster(token[i]);
				this.cluster.add(clustObj);
		}
		this.lookup = lookup;
	}
	
	
	public MBR getMbr() {
		return mbr;
	}
	
	
	
	public double getEsitimatedCostDayHistogram(String startDay,String endDay) throws ParseException{
		double cardinality = 0;
		Map<String, String> days = this.lookup.getTweetsDayIndex(startDay,endDay);
		Iterator it = days.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String,String> obj = (Map.Entry) it.next();
			for(Cluster c : this.cluster){
				if(c.contains(obj.getKey())){
					cardinality +=  c.getOverallMean();
				}
			}
			
		}
		
	    return cardinality;
	}
	
	
	public double getEsitimatedCostWeekHistogram(String startDay,String endDay) throws ParseException{
		double cardinality = 0;
		Map<Week, String> days = this.lookup.getTweetsWeekIndex(startDay, endDay);
		Iterator it = days.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Week,String> obj = (Map.Entry) it.next();
			for(Cluster c : this.cluster){
				if(c.contains(obj.getKey().toString())){
					cardinality +=  c.getOverallMean();
				}
			}
			
		}
		
	    return cardinality;
	}
	
	
	public double getEsitimatedCostMonthHistogram(String startDay,String endDay) throws ParseException{
		double cardinality = 0;
		List<String> months = this.lookup.getTweetsMonth(startDay, endDay);
		for(String month : months){
			for(Cluster c : this.cluster){
				if(c.contains(month)){
					cardinality +=  c.getOverallMean();
				}
			}
			
		}
		
	    return cardinality;
	}
	
	
	public static void main(String[] args){
		String v = "maxlon -179.0 maxlat -89.0 minlon -180.0 minlat -90.0;2014-01-21_2014-04-10_2014-01-09_2014-05-05_2014-04-24_80839.8_0.9800000190734863;2014-10-03_2014-10-18_2014-10-06_2014-04-03_2014-05-31_84554.2_0.9700000286102295;2014-03-31_2014-06-23_2014-02-28_2014-04-08_2014-03-25_2014-03-28_85586.33333333333_0.9900000095367432;2014-05-25_2014-02-14_2014-04-19_2014-02-06_2014-03-24_2014-01-10_2014-04-06_86525.28571428572_0.9900000095367432;2014-10-13_2014-01-23_2014-05-01_2014-02-18_2014-01-28_2014-09-14_87348.16666666666_0.9800000190734863;2014-08-02_2014-09-01_2014-08-28_2014-07-22_2014-04-23_2014-10-19_2014-10-04_88236.28571428572_0.9900000095367432;2014-04-16_2014-05-04_2014-06-18_2014-09-26_2014-09-22_2014-05-03_89092.0_0.9800000190734863;2014-08-27_2014-04-20_2014-08-31_2014-05-10_2014-05-09_2014-04-14_89562.83333333334_0.9700000286102295;2014-04-29_2014-09-25_2014-06-10_2014-03-14_2014-04-17_2014-05-14_90388.83333333334_0.9800000190734863;2014-03-11_2014-04-25_2014-05-21_2014-05-23_2014-09-04_2014-07-17_2014-05-15_91577.42857142857_0.9900000095367432;2014-06-24_2014-04-30_2014-10-05_2014-05-13_2014-07-16_91996.8_0.9800000190734863;2014-06-26_2014-09-12_2014-08-30_2014-05-22_2014-02-26_2014-05-02_2014-09-17_92376.42857142858_0.9800000190734863;2014-05-29_2014-04-26_2014-06-19_2014-09-08_2014-04-18_2014-09-21_93064.33333333334_0.9900000095367432;2014-09-06_2014-05-20_2014-05-11_2014-10-15_2014-09-02_2014-02-12_2014-04-27_93700.57142857142_0.9900000095367432;2014-04-21_2014-05-12_2014-05-19_2014-05-24_2014-04-28_94909.0_0.9800000190734863;2014-05-27_2014-04-22_2014-04-15_2014-10-17_2014-02-24_2014-03-03_95820.16666666666_0.9800000190734863;2014-01-01_2014-06-08_2014-10-07_2014-06-20_97692.0_0.9399999976158142";
		HistogramCell obj = new HistogramCell(v, new Lookup());

	}
	

}
