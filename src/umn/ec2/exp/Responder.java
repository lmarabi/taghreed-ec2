package umn.ec2.exp;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//Someone interested in "Hello" events
public class Responder implements TimeListener {
	private List<Integer> executionTimes = new ArrayList<Integer>();
	
	@Override
	public void reportTime(String time) {
		String[] token = time.split(" ");
		this.executionTimes.add(Integer.parseInt(token[token.length-2]));
		System.out.println("$$$$$$$$$$$$$$$$$$$$$Execution time:" + token[token.length-2]);
	}
	
	public int getExecutionTimes() {
		int sum = 0;
		for(int v  : this.executionTimes){
			sum += v;
		}
		return sum;
	}
	
}