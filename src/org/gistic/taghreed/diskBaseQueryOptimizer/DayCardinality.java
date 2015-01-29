package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DayCardinality implements Comparable<DayCardinality>{
	private String day;
	private long cardinality;
	private int clusterId;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public DayCardinality() {
		// TODO Auto-generated constructor stub
	}
	
	public DayCardinality(String day, long cardinality) {
		this.day = day;
		this.cardinality = cardinality;
	}
	
	public long getCardinality() {
		return cardinality;
	}
	
	public String getDay() {
		return day;
	}
	
	public int getClusterId() {
		return clusterId;
	}
	
	public void setClusterId(int clusterId) {
		this.clusterId = clusterId;
	}
	
	public void parse(String fromText){
		String[]temp  = fromText.split("_");
		this.day = temp[0];
		this.cardinality = Long.parseLong(temp[1]);
	}
	
	@Override
	public String toString() {
		return day+"_"+cardinality;
	}

	@Override
	public int compareTo(DayCardinality arg0){
		//compare based on the cardinality volume 
		return (int) (cardinality - arg0.cardinality);

//		//compare based on the day date

//		try {
//			Date dayobj;
//			dayobj = sdf.parse(day);
//			Date dayarg0 = sdf.parse(arg0.day);
//			return dayobj.compareTo(dayarg0);
//			
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return 0;
		
	}
	
	
	
	

}
