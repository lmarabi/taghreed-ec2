/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.gistic.taghreed.Commons;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.ActiveUsers;
import org.gistic.taghreed.collections.PopularHashtags;
import org.gistic.taghreed.collections.PopularUsers;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.collections.TweetVolumes;
import org.gistic.taghreed.diskBaseQuery.query.DayQueryProcessor;
import org.gistic.taghreed.diskBaseQuery.query.Lookup;
import org.gistic.taghreed.diskBaseQuery.query.Queryoptimizer;
import org.gistic.taghreed.diskBaseQueryOptimizer.GridCell;

/**
 *
 * @author turtle
 */
public class ServerRequest {

	private List<TweetVolumes> dayVolumes;
	private String rtreeDir;
	private String invertedDir;
	private String maxLat;
	private String maxLon;
	private String minLat;
	private String minLon;
	private String startDate;
	private String endDate;
	private String query;
	private String outputResult;
	private MBR mbr;
	private queryType type;
	private queryIndex index;
	private List<PopularHashtags> popularHashtagsResult = new ArrayList<PopularHashtags>();
	private List<ActiveUsers> activePeopleResult;
	private List<PopularUsers> popularPeopleResult;
	private static Lookup lookup = new Lookup();

	public enum queryIndex {

		rtree, inverted
	};

	public enum queryType {

		tweet, hashtag
	};
	
	public enum queryLevel {

		Day,Week,Month
	};

	public ServerRequest(int requestCounter) throws FileNotFoundException, IOException,
			ParseException {
		Commons conf = new Commons();
		this.rtreeDir = conf.getQueryRtreeIndex();
		this.invertedDir = conf.getQueryInvertedIndex();
		outputResult = System.getProperty("user.dir")
				+ "/export/result"+requestCounter+".txt";
		
	}
	
	public static Lookup getLookup() {
		return lookup;
	}

	public void setType(queryType type) {
		this.type = type;
	}

	public void setIndex(queryIndex index) throws FileNotFoundException, IOException, ParseException {
		this.index = index;
		this.loadLookupTables();
	}

	public queryIndex getIndex() {
		return index;
	}

	public queryType getType() {
		return type;
	}

	/**
	 * This method return the TweetVolumes in a sorted list
	 * @return
	 */
	public List<TweetVolumes> getDayVolumes() {
		Collections.sort(this.dayVolumes);
		return this.dayVolumes;
	}

	public void setDayVolumes(List<TweetVolumes> dayVolumes) {
		this.dayVolumes = dayVolumes;
	}

	public String getRtreeDir() {
		return rtreeDir;
	}

	public void setRtreeDir(String rtreeDir) {
		this.rtreeDir = rtreeDir;
	}

	public String getInvertedDir() {
		return invertedDir;
	}

	public void setInvertedDir(String invertedDir) {
		this.invertedDir = invertedDir;
	}

	public void setMBR(String maxLat, String maxlon, String minLat,
			String minLon) {
		this.maxLat = maxLat;
		this.maxLon = maxlon;
		this.minLat = minLat;
		this.minLon = minLon;
		this.mbr = new MBR(new Point(this.maxLat, this.maxLon), new Point(
				this.minLat, this.minLon));
	}

	public String getMaxLat() {
		return maxLat;
	}

	public void setMaxLat(String maxLat) {
		this.maxLat = maxLat;
	}

	public String getMaxLon() {
		return maxLon;
	}

	public void setMaxLon(String maxLon) {
		this.maxLon = maxLon;
	}

	public String getMinLat() {
		return minLat;
	}

	public void setMinLat(String minLat) {
		this.minLat = minLat;
	}

	public String getMinLon() {
		return minLon;
	}

	public void setMinLon(String minLon) {
		this.minLon = minLon;
	}

	public MBR getMbr() {
		return mbr;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getOutputResult() {
		return outputResult;
	}

	public void setOutputResult(String outputResult) {
		this.outputResult = outputResult;
	}
	
	/**
	 * This method read the result from the output file and filter on the fly the 
	 * Active users, popular users, and popular hashtags. 
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public List<Tweet> ReadTheoutputResult() throws NumberFormatException, IOException{
		List<Tweet> tweet = new ArrayList<Tweet>();
		HashMap<String, PopularHashtags> popularHashtags = new HashMap<String, PopularHashtags>();
		HashMap<String, ActiveUsers> activePeople = new HashMap<String, ActiveUsers>();
		HashMap<String, PopularUsers> popularPeople = new HashMap<String, PopularUsers>();
		BufferedReader reader;
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(
				outputResult), "UTF-8"));
		String temp;
		while ((temp = reader.readLine()) != null) {
			String[] attr = temp.split(",");
			try {
				Tweet tweetobj = new Tweet(attr[0], attr[1], attr[2], attr[3],
						attr[4], Integer.parseInt(attr[5]), attr[6], attr[7],
						attr[8], attr[9]);
				tweet.add(tweetobj);
				//Get active people and popular users on the fly
				if (activePeople.containsKey(tweetobj.screenName)) {
					activePeople.get(tweetobj.screenName).tweetCount++;
					popularPeople.get(tweetobj.screenName).followersCount = Math.max(
							popularPeople.get(tweetobj.screenName).followersCount,
							tweetobj.followersCount);
				} else {
					activePeople.put(tweetobj.screenName, new ActiveUsers(
							tweetobj.screenName, 1));
					popularPeople.put(tweetobj.screenName, new PopularUsers(
							tweetobj.screenName, tweetobj.followersCount));
				}
				
				//Get popular Hashtags on the fly.
				try {
					if (tweetobj.tweetText.contains("#")) {
						String[] token = tweetobj.tweetText.split(" ");
						for (int i = 0; i < token.length; i++) {
							//Match the hashtags with the regular expression
							if (token[i].matches("^#[\\p{L}\\p{N}\\p{M}]+")) {
								if (popularHashtags.containsKey(token[i])) {
									popularHashtags.get(token[i]).hashtagCount++;
								} else {
									popularHashtags.put(token[i],new PopularHashtags(token[i], 1));
								}
							}
						}
					}
				} catch (ArrayIndexOutOfBoundsException e) {
				}

			} catch (ArrayIndexOutOfBoundsException e) {
			}

		}
		//popular hashtas 
		popularHashtagsResult = new ArrayList<PopularHashtags>(popularHashtags.values());
		Collections.sort(popularHashtagsResult);
		//Active people 
		activePeopleResult = new ArrayList<ActiveUsers>(
				activePeople.values());
		Collections.sort(activePeopleResult);
		//popular users
		popularPeopleResult = new ArrayList<PopularUsers>(
				popularPeople.values());
		Collections.sort(popularPeopleResult);
		//Delete file after has been read
		File f = new File(outputResult);
		f.delete();
		return tweet;
	}

	/**
	 * This method query Tweets from R+tree index using one only Day Level
	 *
	 * @return List<Tweet>
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws ParseException
	 */
	public List<Tweet> getTweetsRtreeDays() throws FileNotFoundException,
			UnsupportedEncodingException, IOException, ParseException {
		dayVolumes = new ArrayList<TweetVolumes>();
		this.type = queryType.tweet;
		this.index = index.rtree;
		this.loadLookupTables();
		DayQueryProcessor queryProcessor = new DayQueryProcessor(this);
		dayVolumes = queryProcessor.executeQuery();
		return ReadTheoutputResult();
	}
	
	private void loadLookupTables() throws FileNotFoundException, IOException, ParseException{
		//Load lookuptabe
        if(this.index.equals(queryIndex.rtree)){
            lookup.loadLookupTableToArrayList(this.rtreeDir);
        }else{
            lookup.loadLookupTableToArrayList(this.invertedDir);
        }
	}
	
	/**
	 * This method return the Grid Cell used by the query optimizers 
	 * @return GridCell
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws ParseException
	 */
	public GridCell getMasterRtreeDays(queryLevel level) throws FileNotFoundException, IOException, ParseException{
		//If the building grid cell for days otherwise use the else to build 
		// gird cell for weeks or month query level.
		if(level.equals(queryLevel.Day)){
			DayQueryProcessor queryProcessor = new DayQueryProcessor(this);
			return queryProcessor.readMastersFile();
		}else {
			Queryoptimizer queryoptimizer = new Queryoptimizer(this);
			return queryoptimizer.readMastersFile(level);
		}
		
	}
	
	
	
	

	/**
	 * This method query tweets from InvertedIndex using only Day Level
	 *
	 * @return List<Tweet>
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws ParseException
	 */
	public List<Tweet> getTweetsInvertedDay()
			throws UnsupportedEncodingException, FileNotFoundException,
			IOException, ParseException {
		List<Tweet> tweet = new ArrayList<Tweet>();
		dayVolumes = new ArrayList<TweetVolumes>();
		HashMap<String, PopularHashtags> popularHashtags = new HashMap<String, PopularHashtags>();
		this.type = queryType.tweet;
		this.index = index.inverted;
		DayQueryProcessor queryProcessor = new DayQueryProcessor(this);
		return ReadTheoutputResult();
	}
/*
	
	public List<PopularHashtags> getHashtagsRtreeDays()
			throws FileNotFoundException, UnsupportedEncodingException,
			IOException, ParseException {
		HashMap<String, PopularHashtags> popularHashtags = new HashMap<String, PopularHashtags>();
		this.type = type.hashtag;
		this.index = index.rtree;
		DayQueryProcessor dayQueryProcessor = new DayQueryProcessor(this);
		dayQueryProcessor.executeQuery();
		// extractor.Queryoptimizer.main(args);
		BufferedReader reader;
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(
				outputResult), "UTF-8"));
		String temp;
		while ((temp = reader.readLine()) != null) {
			String[] attr = temp.split(",");
			try {
				if (popularHashtags.containsKey(attr[2])) {
					popularHashtags.get(attr[2]).hashtagCount++;
				} else {
					popularHashtags.put(attr[2],
							new PopularHashtags(attr[2], 1));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
			}
		}
		List<PopularHashtags> popularHashtagsResult = new ArrayList<PopularHashtags>(
				popularHashtags.values());
		Collections.sort(popularHashtagsResult);
		return popularHashtagsResult;
	}

	
	public List<PopularHashtags> getHashtagsInvertedDays()
			throws FileNotFoundException, UnsupportedEncodingException,
			IOException, ParseException {
		HashMap<String, PopularHashtags> popularHashtags = new HashMap<String, PopularHashtags>();
		this.type = type.hashtag;
		this.index = index.inverted;
		DayQueryProcessor dayQueryProcessor = new DayQueryProcessor(this);
		dayQueryProcessor.executeQuery();
		// extractor.Queryoptimizer.main(args);
		BufferedReader reader;
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(
				outputResult), "UTF-8"));
		String temp;
		while ((temp = reader.readLine()) != null) {
			String[] attr = temp.split(",");
			try {
				if (popularHashtags.containsKey(attr[2])) {
					popularHashtags.get(attr[2]).hashtagCount++;
				} else {
					popularHashtags.put(attr[2],
							new PopularHashtags(attr[2], 1));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
			}
		}
		List<PopularHashtags> popularHashtagsResult = new ArrayList<PopularHashtags>(
				popularHashtags.values());
		Collections.sort(popularHashtagsResult);
		return popularHashtagsResult;
	}
*/
	/**
	 * This method query Tweets from several level {Day,Week,Month}
	 *
	 * @return List<Tweet>
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws ParseException
	 */
	public List<Tweet> getTweetsRtreePyramid() throws FileNotFoundException,
			UnsupportedEncodingException, IOException, ParseException {
		List<Tweet> tweet = new ArrayList<Tweet>();
		dayVolumes = new ArrayList<TweetVolumes>();
		this.type = type.tweet;
		this.index = index.rtree;
		Queryoptimizer queryoptimizer = new Queryoptimizer(this);
		dayVolumes = queryoptimizer.executeQuery();
		return ReadTheoutputResult();
	}

/*	
	public List<PopularHashtags> getHashtagsRtreePyramid()
			throws FileNotFoundException, UnsupportedEncodingException,
			IOException, ParseException {
		HashMap<String, PopularHashtags> popularHashtags = new HashMap<String, PopularHashtags>();
		String[] args = new String[11];
		this.type = type.hashtag;
		this.index = index.rtree;
		Queryoptimizer queryoptimizer = new Queryoptimizer(this);
		queryoptimizer.executeQuery();
		// extractor.Queryoptimizer.main(args);
		BufferedReader reader;
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(
				outputResult), "UTF-8"));
		String temp;
		while ((temp = reader.readLine()) != null) {
			String[] attr = temp.split(",");
			try {
				if (popularHashtags.containsKey(attr[2])) {
					popularHashtags.get(attr[2]).hashtagCount++;
				} else {
					popularHashtags.put(attr[2],
							new PopularHashtags(attr[2], 1));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
			}
		}
		List<PopularHashtags> popularHashtagsResult = new ArrayList<PopularHashtags>(
				popularHashtags.values());
		Collections.sort(popularHashtagsResult);
		return popularHashtagsResult;
	}
*/
	/**
	 * This method query tweets from severals level [day,week,month] using
	 * inverted index
	 * 
	 * @return List<Tweet>
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws ParseException
	 */
	public List<Tweet> getTweetsInvertedPyramid() throws FileNotFoundException,
			UnsupportedEncodingException, IOException, ParseException {
		List<Tweet> tweet = new ArrayList<Tweet>();
		dayVolumes = new ArrayList<TweetVolumes>();
		this.type = type.tweet;
		this.index = index.inverted;
		Queryoptimizer queryoptimizer = new Queryoptimizer(this);
		dayVolumes = queryoptimizer.executeQuery();
		return ReadTheoutputResult();
	}

/*
	public List<PopularHashtags> getHashtagsInvertedPyramid()
			throws FileNotFoundException, UnsupportedEncodingException,
			IOException, ParseException {
		HashMap<String, PopularHashtags> popularHashtags = new HashMap<String, PopularHashtags>();
		String[] args = new String[11];
		this.type = type.hashtag;
		this.index = index.inverted;
		Queryoptimizer queryoptimizer = new Queryoptimizer(this);
		queryoptimizer.executeQuery();
		// extractor.Queryoptimizer.main(args);
		BufferedReader reader;
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(
				outputResult), "UTF-8"));
		String temp;
		while ((temp = reader.readLine()) != null) {
			String[] attr = temp.split(",");
			try {
				if (popularHashtags.containsKey(attr[2])) {
					popularHashtags.get(attr[2]).hashtagCount++;
				} else {
					popularHashtags.put(attr[2],
							new PopularHashtags(attr[2], 1));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
			}
		}
		List<PopularHashtags> popularHashtagsResult = new ArrayList<PopularHashtags>(
				popularHashtags.values());
		Collections.sort(popularHashtagsResult);
		return popularHashtagsResult;
	}

	public List<PopularHashtags> getHscreenNameashtagsDay()
			throws FileNotFoundException, UnsupportedEncodingException,
			IOException, ParseException {
		HashMap<String, PopularHashtags> popularHashtags = new HashMap<String, PopularHashtags>();
		String[] args = new String[11];
		args[0] = rtreeDir;// HASHTAG_FOLDER;
		args[1] = System.getProperty("user.dir") + "/export/";
		args[2] = maxLat;
		args[3] = maxLon;
		args[4] = minLat;
		args[5] = minLon;
		args[6] = startDate;
		args[7] = endDate;
		args[8] = "hashtag";
		args[9] = "multi";
		args[10] = query;
		DayQueryProcessor dayQueryProcessor = new DayQueryProcessor(this);
		dayQueryProcessor.executeQuery();
		// extractor.Queryoptimizer.main(args);
		BufferedReader reader;
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(
				outputResult), "UTF-8"));
		String temp;
		while ((temp = reader.readLine()) != null) {
			String[] attr = temp.split(",");
			try {
				if (popularHashtags.containsKey(attr[2])) {
					popularHashtags.get(attr[2]).hashtagCount++;
				} else {
					popularHashtags.put(attr[2],
							new PopularHashtags(attr[2], 1));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
			}
		}
		List<PopularHashtags> popularHashtagsResult = new ArrayList<PopularHashtags>(
				popularHashtags.values());
		Collections.sort(popularHashtagsResult);
		return popularHashtagsResult;
	}

	public List<Tweet> getKeywordSearchFast(List<Tweet> tweets, String query) {
		System.out.println("Getting Keyword Search Result ...........");
		List<Tweet> tweetWithKeyword = new ArrayList<Tweet>();
		for (Tweet tweet : tweets) {
			if (tweet.tweetText.contains(query)) {
				tweetWithKeyword.add(tweet);
			}
		}
		return tweetWithKeyword;
	}
*/	
	public List<PopularHashtags> getHashtags(){
		return popularHashtagsResult;
	}

	/**
	 * This method return the Active user measured by the number of user tweets 
	 * @return
	 */
	public List<ActiveUsers> getActiveUser(){
		return activePeopleResult;

	}

	/**
	 * This method return the popular users measured by the number of follower 
	 * For example a higher number of follower is a most popular users.
	 * @return
	 */
	public List<PopularUsers> getPopularUsers(){
		return popularPeopleResult;
	}

/*
	public List<TweetVolumes> getTweetVolumesFast(List<Tweet> tweets)
			throws ParseException {
		System.out.println("Getting Tweet Volumes .......");
		HashMap<String, TweetVolumes> dayVolume = new HashMap<String, TweetVolumes>();
		for (Tweet tweet : tweets) {
			String[] key = tweet.created_at.split(" ");
			if (dayVolume.containsKey(key[0])) {
				dayVolume.get(key[0]).volume++;
			} else {
				// dayVolume.put(key[0], new TweetVolumes(key[0], 1));
			}

		}
		List<TweetVolumes> dayVolumeResult = new ArrayList<TweetVolumes>(
				dayVolume.values());
		Collections.sort(dayVolumeResult);
		return dayVolumeResult;
	}
*/
	List<Tweet> getTopKtweets(int topkInt, List<Tweet> tweetsResult) {
		List<Tweet> result = new ArrayList<Tweet>();
		try {
			if (topkInt > tweetsResult.size()) {
				return tweetsResult;
			} else {
				int quad = topkInt / 4;
				int mid = tweetsResult.size() / 2;
				int subset = 1250;
				int lastindex = tweetsResult.size() - 1;
				result = tweetsResult.subList(0, subset);
				result.addAll(tweetsResult.subList(quad, (quad + subset)));
				result.addAll(tweetsResult.subList(mid, (mid + subset)));
				result.addAll(tweetsResult.subList((lastindex - subset),
						lastindex));
				System.out.println("$$$$$$$$ Tweets subset size:"
						+ result.size());
				System.out
						.println("topk = " + topkInt + "\n" + "mid = " + mid
								+ "\n" + "quad = " + quad + "\n" + "subset = "
								+ subset);
				return result;
			}
		} catch (Exception e) {
			return tweetsResult;
		}
	}

}
