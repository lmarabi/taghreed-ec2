/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Collections;

/**
 *
 * @author louai
 */
public class Test {
	
	static int threashold = 90;

	public static void main(String[] arg) throws IOException,
			FileNotFoundException, ParseException {
		// ServerRequest req = new ServerRequest(1);
		// String maxlat = "90";
		// String maxlon = "180";
		// String minlat = "-90";
		// String minlon = "-180";
		// req.setMBR(maxlat, maxlon, minlat, minlon);
		// req.setStartDate("2014-05-01");
		// req.setEndDate("2014-05-30");
		// req.setQuery("اجمل");
		// List<Tweet> tweetsResult;
		//
		// long starttime = System.currentTimeMillis();

		// tweetsResult = req.getTweetsInvertedDay();

		//
		// long endtime = System.currentTimeMillis();
		// System.out.println("*******************************");
		// Iterator it = tweetsResult.iterator();
		// while(it.hasNext()){
		// Tweet t = (Tweet)it.next();
		// System.out.println(t.toString());
		// }
		// System.err.println("Execution time in milliSecond :"+
		// (endtime-starttime));
		// System.out.println(tweetsResult.size());
//		 int[] list = { 23745, 68498, 70423, 76777, 79024, 79632, 80447,
//		 83454,
//		 83531, 83856, 83892, 83948, 83977, 84208, 84556, 84694, 84769,
//		 84937, 84977, 85079, 85246, 85254, 85339, 85487, 85493, 85613,
//		 85678, 85757, 85816, 85869, 85967, 85972, 86009, 86061, 86163,
//		 86191, 86222, 86228, 86231, 86283, 86390, 86425, 86433, 86481,
//		 86556, 86592, 86622, 86643, 86734, 86826, 86850, 86872, 86996,
//		 87029, 87110, 87132, 87154, 87208, 87257, 87325, 87349, 87354,
//		 87361, 87361, 87386, 87398, 87463, 87545, 87645, 87647, 87656,
//		 87664, 87787, 87910, 87962, 88056, 88065, 88173, 88224, 88265,
//		 88327, 88373, 88393, 88622, 88631, 88650, 88702, 88764, 88794,
//		 88832, 88925, 88972, 89137, 89186, 89199, 89622, 89636, 89651,
//		 89686, 89777, 89872, 90098, 90110, 90295, 90351, 90469, 90534,
//		 90579, 90941, 91143, 91371, 91450, 91450, 91519, 92081, 92164,
//		 92168, 92679, 92704, 93049, 93261, 93266, 93318, 93356, 93371,
//		 93393, 93492, 93503, 93537, 93906, 93919, 94270, 94287, 94297,
//		 94337, 94464, 94473, 95048, 95422, 95538, 95986, 157928,
//		 176462, 177013, 177929, 178818, 179366, 180369, 180762, 182738,
//		 182993, 186371, 187326, 187436, 187464, 188218, 188410, 188477,
//		 190457, 190834, 190849, 190938, 191400, 191403, 195875, 197334,
//		 198080, 266951, 268492, 268871, 273104, 273893, 274722, 277160,
//		 277730, 278162, 279015, 279354, 279379, 279500, 281245, 282116,
//		 282453, 282864, 285107, 287975 };

		int list[] = { 3865501, 3874095, 3391980, 3285100, 3305871, 3613820,
				1370696, 10356034, 2670001, 10311438, 1073674, 2440247,
				10637763, 10262425, 85000, 10032927, 3427647, 9855604,
				3888097, 3880475, 3885796, 3880711, 3892206, 3905085, 1671743,
				3892540, 3891531, 3906542, 3925786, 1419179, 3878740, 3232299,
				2651426, 3895083, 3889511, 3878782, 8749523, 7988664, 8311486,
				10401450, 8748231, 8006068, 2337402, 1313803, 3905044,
				2160546, 3904842, 7707960, 3852912, 3901226, 8196295, 3976852,
				3980982, 1147832, 3857443, 10288813, 10337337, 2729463,
				2688258, 2506064, 3431434, 3287131, 3262191, 2040595, 3293658,
				8846070, 8550072, 9101547, 8259995, 8392175, 8842980, 8586536,
				9292598, 8549377, 8376328, 8698612, 8637350, 8690772, 8706138,
				7768231, 8851002, 7835816, 8471656, 2279608, 5971538, 8126266,
				6380869, 2268711, 2238746, 4120337, 3911041, 3909453, 3875029,
				3748973, 3855006, 3842566, 7773068, 6048348, 9247688, 8068147,
				8980960, 7766566, 7562414, 7791095, 7809269, 3809115, 4918828,
				10220109, 10748020, 10786171, 9961783, 8466172, 4603397,
				8434913, 8962330, 9493437, 9598736, 7583665, 8013383, 8480698,
				7913469, 7757685, 7807006, 6583089, 6861288, 7688685, 8134888,
				9353992, 9593193, 9962140, 9510600, 10525902, 9873923, 9925426,
				9333079, 9747597, 10451225, 9650729, 3110469, 2467485, 4000308,
				409546, 3576697, 23745, 3993034, 2994903, 3024745, 9832251,
				3994424, 9314371, 9559214, 10114927, 2288962, 10712916,
				10539792, 10508274, 10980703, 9957616, 9994726, 2430242,
				10457294, 10547895, 10332303, 10542207, 10443576, 4627617,
				10405365, 10454337, 10039603, 10565809, 10144131, 5956845,
				9998009, 9964990, 8347935, 8576949, 9298253, 7925161, 7278830,
				8466664, 3934446, 3948782, 3933868, 3936148, 3928303, 3963715,
				3935119, 3928078, 3939184, 3384110, 7369216, 3850640,
				8079406, 8349756, 3054636, 7938428, 3826384, 3382394, 8056231,
				9463295, 9109780, 844208, 3954494, 3958173, 3057662, 1095995,
				3981487, 118811, 2741870, 3552460, 3283694, 452486, 9214167,
				9174242, 9354295, 9210195, 7898361, 8110014, 6493648, 4181304,
				8045151, 7707394, 7925322, 7804915, 8541981, 9201191 };
		Arrays.sort(list);
		int clusterdDay= 0 ;
		int count =0;
		for(int i: list){
			for(int j: list){
				if(i ==j){
					count++;
				}if(i ==j && count > 1){
					System.out.println("Match Found: "+ count);
				}
			}
			count = 0;
		}
		List<Integer> seeds = new ArrayList<Integer>();
		int previous = 0;
		for (int i = 1; i < list.length; i++) {

			// ( | V1 - V2 | / ((V1 + V2)/2) ) * 100
//			int V1 = list[i];
//			int V2 = list[i + 1];
//			int preValue = list[previous];
//			double gapInc = calculateGapPercentage(V1, V2);
//			double gapPrevious = calculateGapPercentage(preValue, V2);
//			 if(gapInc > 15 || gapPrevious >15){
//			 seeds.add(i+1);
//			 previous = i+1;
//			 }
			 
			if (isClusterExist(list, previous, i)) {
				seeds.add(previous);
				seeds.add(i-1);
				previous = i;
			}
		}
		if(seeds.get(seeds.size()-1) != list.length){
			seeds.add(previous);
			seeds.add(list.length-1);
//			seeds.set(seeds.size()-1, list.length-1);
		}
		// init cluster statistics
		List<Integer> cluster = new ArrayList<Integer>();
		double avg;
		double deviation;
		double SD;
		double SE;
		System.out.println("Number of clusters found: " + ((int) seeds.size()/2) + " Number of days:"+list.length);
		// print the fist cluster

		int counter=1;
		for (int seed = 0; seed < seeds.size() - 1; seed = seed+2) {
			System.out.println("\nCluster ( " + (counter++) + " ) from:"
					+ seeds.get(seed)+" - To:"+seeds.get(seed+1));
			cluster = copyToCluster(seeds.get(seed), seeds.get(seed+1), list);
			clusterdDay += cluster.size();
			for(Integer i : cluster)
				System.out.print(i+",");
			System.out.println("\nMean: " + getAvrage(cluster)
					+ "Sample Size: " + cluster.size());
			System.out.println("SD: " + getStandardDeviation(cluster)
					+ " - RelativeSD: %" + getratioStandardDeviation(cluster));
			System.out.println("SE: " + getStandardError(cluster)
					+ " - RelativeSE: %" + getRelativeStandardError(cluster));
			System.out.println("Confidence: %"+getPersentofConfidenceInterval(cluster));
			cluster.clear();
		}

		System.out.println("Input day:"+list.length+" clusted day:"+ clusterdDay);
	}
	
	private static List<Integer> copyToCluster(int from , int to, int[] list){
		List<Integer> cluster = new ArrayList<Integer>();
		while( from <= to){
			cluster.add(list[from]);
			from++;
		}
		return cluster;
	}

	private static boolean isClusterExist(int[] array, int from, int to) {
		List<Integer> list = new ArrayList<Integer>();
		
		while (from <= to) {
			list.add(array[from]);
			from++;
		}
		double confidence = getPersentofConfidenceInterval(list);
		if(confidence > threashold)
			return false;
		else
			return true;
	}

	private static double calculateGapPercentage(int value1, int value2) {
		int x = Math.abs((value1 - value2));
		double y = (value1 + value2);
		y /= 2;
		double gap = x / y;
		gap *= 100;
		System.out.println("v1:" + value1 + " v2:" + value2 + " gap:" + gap);
		return gap;
	}

	private static double getAvrage(List<Integer> list) {
		int sum = 0;
		for (Integer i : list) {
			sum += i;
		}
		sum /= list.size();
		return sum;
	}

	private static double getDeviation(List<Integer> list) {
		double deviation = 0;
		double avg = getAvrage(list);
		for (Integer i : list) {
			deviation += Math.pow((i - avg), 2);
		}
		return deviation;
	}

	private static double getStandardDeviation(List<Integer> list) {
		double deviation = getDeviation(list);
		double result = (Math.sqrt(deviation / (list.size() - 1)));
		return result;
	}

	private static double getStandardError(List<Integer> list) {
		return (getStandardDeviation(list) / (Math.sqrt(list.size())));
	}

	private static double getratioStandardDeviation(List<Integer> list) {
		double avg = getAvrage(list);
		double result = (getStandardDeviation(list) / avg) * 100;
		return result;
	}

	private static double getRelativeStandardError(List<Integer> list) {
		double avg = getAvrage(list);
		return (getStandardError(list) / avg) * 100;
	}

	private static double getPersentofConfidenceInterval(List<Integer> list) {
		
			int avg = (int) ConfidenceCoefficient.getMean(list);
			int upperbound = 0;
			int lowerbound = 0;
			int max = list.get(list.size() - 1);
			int min = list.get(0);
			double z = 0;
			if(list.size() == 41){
				System.out.println("Here");
			}
			for (int i = 99; i > 0; i--) {
				int ci = (int) ConfidenceCoefficient.calcMeanCI(list, i);
				upperbound = avg + ci;
				lowerbound = avg - ci;
				if (upperbound >= max && lowerbound <= min) {
					return i;
				}
			}
		
		return 0;

	}

	private static double getLowerConfidenceInterval(List<Integer> list) {
		double ci = getAvrage(list);
		ci -= (1.96 * getStandardError(list));
		return ci;
	}

}
