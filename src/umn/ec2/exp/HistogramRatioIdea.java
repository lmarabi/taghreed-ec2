package umn.ec2.exp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

public class HistogramRatioIdea {

	List<hDay> day = new ArrayList<hDay>();
	List<hWeek> week = new ArrayList<hWeek>();
	hMonth month = new hMonth();
	int globalRatio = 30;

	/**
	 ******************************************************************************************* 
	 */

	private static class hDay implements Comparable<hDay> {
		int day;
		int execution;
		String plan;

		public hDay(int day, int execution) {
			this.day = day;
			this.execution = execution;
			this.plan = "";
		}

		public void setPlan(String plan) {
			this.plan = plan;
		}

		public String getPlan() {
			return plan;
		}

		public void print() {
			System.out.println("day= " + this.day + "  exe=" + this.execution+" plan="+this.plan);
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
		int execution;
		int volume;

		public hWeek(int start, int end, int execution) {
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
		int execution;

		public hMonth(int start, int end, int execution) {
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
				day.add(new hDay(counter, Integer.parseInt(line)));
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
						.parseInt(temp[1]), Integer.parseInt(temp[2])));
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
						Integer.parseInt(temp[1]), Integer.parseInt(temp[2]));
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
			int executionTime = getDayexecutionRange(0, i);
			System.out.println("(" + 1 + " - " + i  + ") = "
					+ executionTime);
			result.add(new hDay(i, executionTime));
		}
		return result;
	}

	private int getDayexecutionRange(int start, int end) {
		int result = 0;
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
		for (int i = 0; i < this.day.size(); i++) {
			int executionTime = getWeeklexecutionRange(1, i, 1);
			System.out.println("(" + 1 + " - " + i + ") = " + executionTime);
			result.add(new hDay(i, executionTime));
		}
		return result;
	}

	private List<hDay> CalculateMultiLevel() {
		List<hDay> result = new ArrayList<hDay>();
		int executionTime = 0;
		System.out
				.println("\nCalculate multi Level\nStart - end - ExecutionTime ");
		for (int i = 0; i < this.day.size(); i++) {
			executionTime = CalculateMonthLevel(1, i);
			if (executionTime == 0) {
				List<hWeek> tempweek = getWeekRange(1, i);
				System.out.print("\n(" + 1 + " - " + i + ") = ");
				if (tempweek.size() >= 1) {
					for (hWeek w : tempweek) {
						System.out.print(" " + w.startDay + "," + w.endDay);
						executionTime += w.execution;
					}

					Collections.sort(tempweek);
					int begin = tempweek.get(0).startDay;
					int finish = tempweek.get(tempweek.size() - 1).endDay;
					int countDay = i-finish;
					countDay++;
					if (finish != i) {
						executionTime += getDayexecutionRange(finish, i);
						System.out.print(" query from " + finish + " to " + i);
					}
					hDay temp = new hDay(i, executionTime);
					temp.setPlan(countDay + "-" + tempweek.size() + "-0");
					result.add(temp);
				} else {
					// Execute days that are not query from month or week
					executionTime = getDayexecutionRange(1, i);
					System.out.println("query form 1 to" + i);
					hDay temp = new hDay(i, executionTime);
					int countDay = i - 1;
					countDay++;
					temp.setPlan(countDay + "-0-0");
					result.add(temp);
				}
			} else {
				// Execute when month above or equal the selected ratio.
				System.out
						.println("(" + 1 + " - " + i + ") = " + executionTime);
				hDay temp = new hDay(i, executionTime);
				temp.setPlan("0-0-1");
				result.add(temp);

			}
		}
		Collections.sort(result);
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

	private int getWeeklexecutionRange(int start, int end, int ratio) {
		int result = 0;
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

	// **********************
	private int CalculateMonthLevel(int start, int end) {
		if (end == 30)
			System.out.println("&");
		int result = 0;
		int numberofDays = end - start;
		numberofDays++;
		float ratio = ((float) numberofDays / month.volume) * 100;
		int monthRatio = (int) ratio;
		if (monthRatio >= globalRatio) {
			System.out.println("Success the global ratio");
			result = month.execution;
		}
		return result;
	}

	public static void main(String[] args) throws Exception, IOException {

		HistogramRatioIdea test = new HistogramRatioIdea();
		// test.globalRatio = Integer.parseInt(args[0]);
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir") + "/"
						+ test.globalRatio + ".csv", false), "UTF-8");
		test.loadData();
		 for (hDay d : test.day) {
		 d.print();
		 }
		//
		// for (hWeek w : test.week) {
		// w.print();
		// }
		// test.month.print();
		List<hDay> day = test.CalculateDayLevel();
		List<hDay> week = test.CalculateWeekLevel();
		List<hDay> multi = test.CalculateMultiLevel();
		System.out.println("==================Day===================");
		for(hDay d: day)
			d.print();
		System.out.println("===================Week==================");
		for(hDay d: week)
			d.print();
		System.out.println("=================Multi====================");
		for(hDay d: multi)
			d.print();
		System.out.println("dayCount=" + day.size() + "\tweekCount="
				+ week.size() + "\tmultiCount=" + multi.size());
		writer.write("start,end,q.day,q.week,q.month,q.multi\n");
		for (int i = 0; i < day.size(); i++) {
			System.out.println("1," +i + "," + day.get(i).execution
					+ "," + week.get(i).execution + "," + test.month.execution
					+ "," + multi.get(i).execution + ","
					+ multi.get(i).getPlan());
			writer.write("\n1," + i  + "," + day.get(i).execution + ","
					+ week.get(i).execution + "," + test.month.execution + ","
					+ multi.get(i).execution + "," + multi.get(i).getPlan());
		}
		writer.close();

	}

}
