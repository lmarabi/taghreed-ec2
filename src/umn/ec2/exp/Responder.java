package umn.ec2.exp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//Someone interested in "Hello" events
public class Responder implements TimeListener {
	private List<Integer> executionTimes = new ArrayList<Integer>();

	@Override
	public void reportTime(String time) {
		try {
			String[] token = time.split(" ");
			Integer t = 0;
			if (time.matches("Total indexing time in millis \\d+")) {
				t = Integer.parseInt(token[token.length - 1]);
			} else {
				t = Integer.parseInt(token[token.length - 2]);
			}

			this.executionTimes.add(t);
			System.out.println("$$$$$$$$$$$$$$$$$$$$$Execution time:" + t);
		} catch (Exception ex) {
			// handel error.
		}
	}

	public double getAvgExecutionTimes() {
		int sum = 0;
		for (int v : this.executionTimes) {
			sum += v;
		}
		return (double) sum / executionTimes.size();
	}

	public double getTotalExecutionTimes() {
		int sum = 0;
		for (int v : this.executionTimes) {
			sum += v;
		}
		return sum;
	}

}