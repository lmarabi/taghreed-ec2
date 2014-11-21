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
import java.util.Iterator;
import java.util.List;

import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;

/**
 *
 * @author louai
 */
public class Test {

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
		int[] list = { 23745, 68498, 70423, 76777, 79024, 79632, 80447, 83454,
				83531, 83856, 83892, 83948, 83977, 84208, 84556, 84694, 84769,
				84937, 84977, 85079, 85246, 85254, 85339, 85487, 85493, 85613,
				85678, 85757, 85816, 85869, 85967, 85972, 86009, 86061, 86163,
				86191, 86222, 86228, 86231, 86283, 86390, 86425, 86433, 86481,
				86556, 86592, 86622, 86643, 86734, 86826, 86850, 86872, 86996,
				87029, 87110, 87132, 87154, 87208, 87257, 87325, 87349, 87354,
				87361, 87361, 87386, 87398, 87463, 87545, 87645, 87647, 87656,
				87664, 87787, 87910, 87962, 88056, 88065, 88173, 88224, 88265,
				88327, 88373, 88393, 88622, 88631, 88650, 88702, 88764, 88794,
				88832, 88925, 88972, 89137, 89186, 89199, 89622, 89636, 89651,
				89686, 89777, 89872, 90098, 90110, 90295, 90351, 90469, 90534,
				90579, 90941, 91143, 91371, 91450, 91450, 91519, 92081, 92164,
				92168, 92679, 92704, 93049, 93261, 93266, 93318, 93356, 93371,
				93393, 93492, 93503, 93537, 93906, 93919, 94270, 94287, 94297,
				94337, 94464, 94473, 95048, 95422, 95538, 95986, 157928,
				176462, 177013, 177929, 178818, 179366, 180369, 180762, 182738,
				182993, 186371, 187326, 187436, 187464, 188218, 188410, 188477,
				190457, 190834, 190849, 190938, 191400, 191403, 195875, 197334,
				198080, 266951, 268492, 268871, 273104, 273893, 274722, 277160,
				277730, 278162, 279015, 279354, 279379, 279500, 281245, 282116,
				282453, 282864, 285107, 287975 };
		
		List<Integer> seeds = new ArrayList<Integer>();
		int previous = 0;
		for(int i=0 ; i < list.length-1 ; i++){
			
			//( | V1 - V2 | / ((V1 + V2)/2) ) * 100 
			int V1 = list[i];
			int V2 = list[i+1];
			int preValue = list[previous];
			double gapInc = calculateGapPercentage(V1, V2); 
			double gapPrevious = calculateGapPercentage(preValue, V2);
			if(gapInc > 15 || gapPrevious >15){
				seeds.add(i+1);
				previous = i+1;
			}
		}
		
		System.out.println("Number of clusters found: "+seeds.size());
		//print the fist cluster 
		System.out.println("\nCluster ( 1 ) index:0");
		for(int i= 0 ; i < seeds.get(0) ; i++){
			System.out.print(list[i]+",");
		}
		
		for(int seed =0 ; seed < seeds.size()-1; seed++){
			System.out.println("\nCluster ( " +  (seed)+" ) index:"+ seeds.get(seed));
			for(int i= seeds.get(seed) ; i< seeds.get(seed+1) ; i++){
				System.out.print(list[i]+",");
			}
		}
		//print the last cluster 
		System.out.println("\nCluster ( last )");
		for(int i= seeds.get(seeds.size()-1) ; i < list.length ; i++){
			System.out.print(list[i]+",");
		}

	}
	
	private static double calculateGapPercentage(int value1, int value2){
		int x =  Math.abs((value1 - value2));
		double y = (value1 + value2);
		y /= 2;
		double gap = x /y ;
		gap *= 100;
		System.out.println("v1:"+value1+" v2:"+value2+" gap:"+gap);
		return gap;
	}

}
