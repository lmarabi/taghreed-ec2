/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.gistic.invertedIndex.MetaData;
import org.gistic.taghreed.collections.PopularHashtags;
import org.gistic.taghreed.collections.PopularUsers;
import org.gistic.taghreed.collections.Tweet;

/**
 *
 * @author turtle
 */
public class Main {

	public static void main(String[] args) throws FileNotFoundException,
			UnsupportedEncodingException, IOException, ParseException {
		List<PopularHashtags> popularHashtags = new ArrayList<PopularHashtags>();
		List<Tweet> tweets = new ArrayList<Tweet>();
		
		ServerRequest req =  new ServerRequest();
		req.setStartDate("2014-10-20");
		req.setEndDate(req.getStartDate());
		String maxlat = "90";
		String maxlon = "180";
		String minlat = "-90";
		String minlon = "-180";
		req.setMBR(maxlat, maxlon, minlat, minlon);

//		tweets = req.getTweetsRtreeDays();
//		popularHashtags = req.getPopularHashtags();
//		
//		for (Tweet t : tweets) {
//			System.out.println(t.toString());
//		}
//		for (PopularHashtags hash : popularHashtags) {
//			System.out.println(hash.toString());
//		}
		
		
		
		System.out.println("Tweets Size:" + req.getTweetsRtreeDays().size());
		System.out.println("Hashtags Size = " + req.getHashtags().size());
		System.out.println("Active user size= "+ req.getActiveUser().size());
		System.out.println("popular users size= "+ req.getPopularUsers().size());
		
//		for(PopularUsers user : req.getPopularUsers()){
//			System.out.println(user.toString());
//		}

	}

	private void testCall() throws FileNotFoundException, IOException,
			ParseException {
//		ServerRequest req = new ServerRequest();
//		String maxlat = "90";
//		String maxlon = "180";
//		String minlat = "-90";
//		String minlon = "-180";
//		req.setMBR(maxlat, maxlon, minlat, minlon);
//		req.setStartDate("2014-01-06");
//		req.setEndDate("2014-01-06");
//		req.setQuery("adsf");
//		String index = "invereted";
//		String level = "pyramid";
//		List<PopularHashtags> popularHashtags = new ArrayList<PopularHashtags>();
//		List<Tweet> tweets = new ArrayList<Tweet>();

//		if (level.equals("day")) {
//			if (index.equals("rtree")) {
//				tweets = req.getTweetsRtreeDays();
//			} else {
//				tweets = req.getTweetsInvertedDay();
//				popularHashtags = req.getHashtagsInvertedDays();
//			}
//
//		} else {
//			if (index.equals("rtree")) {
//				tweets = req.getTweetsRtreePyramid();
//				popularHashtags = req.getHashtagsRtreePyramid();
//			} else {
//				// query from inverted index
//				tweets = req.getTweetsInvertedPyramid();
//				// popularHashtags = req.getHashtagsInvertedPyramid();
//			}
//
//		}
//
//		for (Tweet t : tweets) {
//			System.out.println(t.toString());
//		}
//		for (PopularHashtags hash : popularHashtags) {
//			System.out.println(hash.toString());
//		}
//
//		System.out.println("Tweets Size:" + tweets.size());
//		System.out.println("Hashtags Size = " + popularHashtags.size());
	}
	
	private void statistics_DeleteMe() throws IOException, ParseException{
		ServerRequest req = new ServerRequest();
		req.setStartDate("2014-05-02");
		req.setEndDate("2014-05-02");
		String resultPath = "/export/scratch/louai/test/index";
		File file = new File(new File(resultPath) + "/_inverted_time");
		if (!file.exists()) {
			file.createNewFile();
		}
		Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(file, true), "UTF8"));
		List<Tweet> tweets = new ArrayList<Tweet>();
		MetaData metaData = new MetaData();
		String indexPath = "/export/scratch/louai/test/index/invertedindex/tweets/Day/index.2014-05-02";
		List<String> query = metaData.getAllKeywordOfIndex(indexPath);
		Integer maxlat = 91;
		Integer maxlon = 181;
		Integer minlat = -91;
		Integer minlon = -181;
		for (int i = 0; i < query.size(); i++) {
			boolean flag = false;
			double inveretdtime =0;
			do {
				System.out.println("==================================================");
				System.out.println("query = "+ query.get(i));
				int doc = 0;
				maxlat = maxlat-20;
				maxlon = maxlon-20;
				minlat = minlat + 20;
				minlon = minlon + 20;
				req.setMBR(maxlat.toString(), maxlon.toString(),
						minlat.toString(), minlon.toString());
				System.out.println("MBR = "+ req.getMbr().toString());
				System.out.println("==================================================");
				req.setQuery(query.get(i));
				writer.append(query.get(i) + "," + minlat + "," + minlon + ","
						+ maxlat + "," + maxlon + ",");
				double starttime = System.currentTimeMillis();
				tweets = req.getTweetsRtreeDays();
				doc = tweets.size();
				double endtime = System.currentTimeMillis();
				double time = endtime - starttime;
				writer.append(time + ",");
				
				if(!flag){
					flag = true;
					starttime = System.currentTimeMillis();
					tweets = req.getTweetsInvertedDay();
					endtime = System.currentTimeMillis();
					inveretdtime = endtime - starttime;
				}
				
				time = endtime - starttime;
				writer.append(inveretdtime + "," + doc+"\n");
			} while (minlat <= 0);
			maxlat = 91;
			maxlon = 181;
			minlat = -91;
			minlon = -181;
			flag = false;
			System.out.println("=================================================="
							+ "Change query and mbr");
		}
		writer.flush();
		writer.close();
	}

}
