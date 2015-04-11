package umn.ec2.exp;

import java.util.ArrayList;
import java.util.List;

public class SamplersCollector implements TimeListener {
	private List<String> sample = new ArrayList<String>();
	
	@Override
	public void reportTime(String result) {
		sample.add(result);
	}
	
	
	
	public  List<String> getSamples() {
		return this.sample;
	}
	
}