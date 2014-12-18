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
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.PopularHashtags;
import org.gistic.taghreed.collections.PopularUsers;
import org.gistic.taghreed.collections.TopTweetResult;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryIndex;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryType;
import org.gistic.taghreed.diskBaseQueryOptimizer.QueryPlanner;

/**
 *
 * @author turtle
 * 
 * To support reading rtree index do the following 
 * Fs.open stream 
 * Rtree r = new Rtree();
 * setstokOjbect(Shape of the Tweet "new Tweets)
 * read Fileds(fs.open obj)
 * search(Rectagnle , Collection<output> )
 * result will return in the output 
 * 
 */
public class Main {

	public static void main(String[] args) throws FileNotFoundException,
			UnsupportedEncodingException, IOException, ParseException, InterruptedException {
		List<PopularHashtags> popularHashtags = new ArrayList<PopularHashtags>();
		List<Tweet> tweets = new ArrayList<Tweet>();
		QueryPlanner queryPlan = new QueryPlanner();
		
		
		ServerRequest req =  new ServerRequest();
		req.setStartDate("2014-05-21");
		req.setEndDate("2014-07-21");
		req.setType(queryType.tweet);
		req.setIndex(queryIndex.rtree);
		MBR mbr = new MBR(new Point(40.694961541009995,118.07045041992582),new Point(38.98904106170265,114.92561399414794) );
		req.setMBR(mbr);
		
		queryLevel queryfrom = queryPlan.getQueryPlan(req.getStartDate(),req.getEndDate(), req.getMbr());
//		queryLevel queryfrom = queryLevel.Day;
		req.setQueryResolution(queryfrom);
		
		double starttime = System.currentTimeMillis();
		req.getTweetsRtreeDays();
		double endtime = System.currentTimeMillis();
		System.out.println("Tweets Size:" + req.getTopKtweets().size());
		System.out.println("Hashtags Size = " + req.getHashtags().size());
		System.out.println("Active user size= "+ req.getActiveUser().size());
		System.out.println("popular users size= "+ req.getPopularUsers().size());
		System.out.println("Execcution Time:"+(endtime-starttime)+" Millis");


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
	
	

}
