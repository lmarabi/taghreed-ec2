package umn.ec2.exp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

public class HistogramRatioIdea {

	List<hDay> day = new ArrayList<hDay>();
	List<hWeek> week = new ArrayList<hWeek>();
	hMonth month = new hMonth();
	int globalRatio = 100;

	/**
	 ******************************************************************************************* 
	 */

	private static class hDay implements Comparable<hDay> {
		int day;
		float execution;

		public hDay(int day, float execution) {
			this.day = day;
			this.execution = execution;
		}

		public void print() {
			System.out.println("day= " + this.day + "  exe=" + this.execution);
		}

		@Override
		public int compareTo(hDay arg0) {
			// TODO Auto-generated method stub
			return this.day - arg0.day;
		}
	}

	/**
	 ******************************************************************************************* 
	 */

	private static class hWeek implements Comparable<hWeek> {
		int startDay;
		int endDay;
		float execution;
		int volume;

		public hWeek(int start, int end, float execution) {
			this.startDay = start;
			this.endDay = end;
			this.execution = execution;
			this.volume = end - start;
			this.volume++;
		}

		private void print() {
			System.out.println("****************\n" + "StartDay= "
					+ this.startDay + "\tEndDay= " + this.endDay + "\tVolume= "
					+ this.volume + "\texe= " + this.execution + "\n"
					+ "****************");
		}

		@Override
		public int compareTo(hWeek arg0) {
			return this.startDay - arg0.startDay;
		}

	}

	/**
	 ******************************************************************************************* 
	 */

	private static class hMonth {
		int start;
		int end;
		int volume;
		float execution;

		public hMonth(int start, int end, float execution) {
			this.start = start;
			this.end = end;
			this.volume = end - start;
			this.volume++;
			this.execution = execution;
		}

		public hMonth() {
			// TODO Auto-generated constructor stub
		}

		private void print() {
			System.out.println("Month(" + this.start + "," + this.end + ") = "
					+ this.execution);
		}

	}

	/**
	 ******************************************************************************************* 
	 */

	private void loadData() {
		BufferedReader reader;
		StringBuilder dayString = new StringBuilder();
		String line = null;
		int counter = 1;
		try {
			reader = new BufferedReader(new FileReader(
					System.getProperty("user.dir") + "/hday.txt"));
			while ((line = reader.readLine()) != null) {
				day.add(new hDay(counter, Float.parseFloat(line)));
				counter++;
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Load week data
		try {
			reader = new BufferedReader(new FileReader(
					System.getProperty("user.dir") + "/hweek.txt"));
			while ((line = reader.readLine()) != null) {
				String[] temp = line.split(",");
				week.add(new hWeek(Integer.parseInt(temp[0]), Integer
						.parseInt(temp[1]), Float.parseFloat(temp[2])));
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Load Month data
		try {
			reader = new BufferedReader(new FileReader(
					System.getProperty("user.dir") + "/hmonth.txt"));
			while ((line = reader.readLine()) != null) {
				String[] temp = line.split(",");
				month = new hMonth(Integer.parseInt(temp[0]),
						Integer.parseInt(temp[1]), Float.parseFloat(temp[2]));
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Collections.sort(week);
		Collections.sort(day);
	}

	// ********************************

	private List<hDay> CalculateDayLevel() {
		List<hDay> result = new ArrayList<hDay>();
		System.out
				.println("\nCalculate Day Level\nStart - end - ExecutionTime ");
		for (int i = 0; i < this.day.size(); i++) {
			float executionTime = getDayexecutionRange(0, i);
			System.out.println("(" + 1 + " - " + (i + 1) + ") = "
					+ executionTime);
			result.add(new hDay(i, executionTime));
		}
		return result;
	}

	private float getDayexecutionRange(int start, int end) {
		float result = 0;
		while (start <= end) {
			result += this.day.get(start).execution;
			start++;
		}
		return result;
	}

	// ********************************

	private List<hDay> CalculateWeekLevel() {
		List<hDay> result = new ArrayList<hDay>();
		System.out
				.println("\nCalculate Week Level\nStart - end - ExecutionTime ");
		for (int i = 1; i < this.day.size(); i++) {
			float executionTime = getWeeklexecutionRange(1, i, 1);
			System.out.println("(" + 1 + " - " + i + ") = " + executionTime);
			result.add(new hDay(i, executionTime));
		}
		return result;
	}

	private List<hDay> CalculateMultiLevel() {
		List<hDay> result = new ArrayList<hDay>();
		System.out
				.println("\nCalculate multi Level\nStart - end - ExecutionTime ");
		for (int i = 1; i < this.day.size() + 1; i++) {
			List<hWeek> tempweek = getWeekRange(1, i);
			System.out.print("\n(" + 1 + " - " + i + ") = ");
			if (tempweek.size() >= 1) {
				for (hWeek w : tempweek) {
					System.out.print(" " + w.startDay + "," + w.endDay);
				}

				Collections.sort(tempweek);
				int begin = tempweek.get(0).startDay;
				int finish = tempweek.get(tempweek.size()-1).endDay;
				if(finish != i) {
					getDayexecutionRange(finish, i);
					System.out.print(" query from "+finish+" to " + i);
				}
			}else{
				System.out.println("query form 1 to"+i);
			}
			// float executionTime = getWeeklexecutionRange(1,i,globalRatio);
			// System.out.println("("+1+" - "+i+") = "+executionTime);
			// result.add(new hDay(i,executionTime));
		}
		return result;
	}

	private List<hWeek> getWeekRange(int start, int end) {
		List<hWeek> result = new ArrayList<hWeek>();
		for (hWeek w : week) {
			if ((start <= w.endDay) && (end >= w.startDay)) {
				int matched = 0;
				int i = w.startDay;
				while (i <= w.endDay) {
					for (int s = start; s <= end; s++) {
						if (i == s)
							matched++;
					}
					i++;
				}

				float localRatio = ((float) ((float) matched / w.volume) * 100);
				int localInteger = (int) (localRatio);
				if (localInteger >= globalRatio) {
					result.add(w);
				}

			}
		}
		return result;
	}

	private float getWeeklexecutionRange(int start, int end, int ratio) {
		float result = 0;
		int numberOfDays = end - start;
		numberOfDays++;
		for (hWeek w : week) {
			if ((start <= w.endDay) && (end >= w.startDay)) {
				int matched = 0;
				int i = w.startDay;
				while (i <= w.endDay) {
					for (int s = start; s <= end; s++) {
						if (i == s)
							matched++;
					}
					i++;
				}

				float localRatio = ((float) ((float) matched / w.volume) * 100);
				int localInteger = (int) (localRatio);
				if (localInteger >= ratio) {
					result += w.execution;
				}

			}
		}
		return result;
	}

	public static void main(String[] args) {
		HistogramRatioIdea test = new HistogramRatioIdea();
		test.loadData();
		for (hDay d : test.day) {
			d.print();
		}

		for (hWeek w : test.week) {
			w.print();
		}

		test.month.print();
		// test.CalculateDayLevel();
		// test.CalculateWeekLevel();
		test.CalculateMultiLevel();

	}

}
