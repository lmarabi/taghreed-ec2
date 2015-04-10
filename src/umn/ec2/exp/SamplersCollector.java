package umn.ec2.exp;

import java.util.ArrayList;
import java.util.List;

public class SamplersCollector implements TimeListener {
	private List<String> sample = new ArrayList<String>();
	
	@Override
	public void reportTime(String result) {
		sample.add(getRectangle(result));
//		this.executionTimes.add(Integer.parseInt(token[token.length-2]));
		System.out.println(result);
	}
	
	private String getRectangle(String point){
		
		double area_ratio = (double)0.01/1000;
		String[] token = point.split(",");
		double x = Double.parseDouble(token[0]);
		double y = Double.parseDouble(token[1]);
		int total_width = 360;
		int total_height = 180;
		double w = Math.sqrt(area_ratio) * total_width;
		double h = Math.sqrt(area_ratio) * total_height;
		double max_x = x + w;
		double max_y = y + h;
		return " rect:"+ x+","+y+","+max_x+","+max_y;
	}
	
	public String getSamples() {
		String temp = "";
		for(String v  : this.sample){
			temp += v;
		}
		return temp;
	}
	
}