package org.gistic.taghreed.collections;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.util.PriorityQueue;

public class TopTweetResult extends PriorityQueue<Tweet> {

	private int capacity;
	private HashMap<String, PopularHashtags> popularHashtags = new HashMap<String, PopularHashtags>();
	private HashMap<String, ActiveUsers> activePeople = new HashMap<String, ActiveUsers>();
	private HashMap<String, PopularUsers> popularPeople = new HashMap<String, PopularUsers>();
	private static Map<Date, Integer> tweetsVolume = new HashMap<Date, Integer>();

	public TopTweetResult(int size) {
		this.capacity = size;
		super.initialize(capacity);

	}

	@Override
	protected boolean lessThan(Object arg0, Object arg1) {
		return ((Tweet) arg0).getPriorityValue() >= ((Tweet) arg1)
				.getPriorityValue();
	}

	@Override
	public boolean insert(Tweet element) {
		boolean overflow = this.size() == this.capacity;
		Random r = new Random();
		int R = r.nextInt(this.capacity - 0) + 0;
		int priorityValue = r.nextInt();
		element.setPriorityValue(priorityValue);
		setStatistics(element);
		return super.insert(element);
	}

	/**
	 * This method set the active people and popular people and hashtags on the
	 * fly.
	 * 
	 * @param tweetobj
	 * @throws ParseException 
	 */
	private void setStatistics(Tweet tweetobj) {
		try {
			// Get active people and popular users on the fly
			if (activePeople.containsKey(tweetobj.screenName)) {
				activePeople.get(tweetobj.screenName).tweetCount++;
				popularPeople.get(tweetobj.screenName).followersCount = Math
						.max(popularPeople.get(tweetobj.screenName).followersCount,
								tweetobj.followersCount);
			} else {
				activePeople.put(tweetobj.screenName, new ActiveUsers(
						tweetobj.screenName, 1));
				popularPeople.put(tweetobj.screenName, new PopularUsers(
						tweetobj.screenName, tweetobj.followersCount));
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}

		// Get popular Hashtags on the fly.
		try {
			if (tweetobj.tweetText.contains("#")) {
				String[] token = tweetobj.tweetText.split(" ");
				for (int i = 0; i < token.length; i++) {
					// Match the hashtags with the regular expression
					if (token[i].matches("^#[\\p{L}\\p{N}\\p{M}]+")) {
						if (popularHashtags.containsKey(token[i])) {
							popularHashtags.get(token[i]).hashtagCount++;
						} else {
							popularHashtags.put(token[i], new PopularHashtags(
									token[i], 1));
						}
					}
				}
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}
		
		//Insert tweets volume 
		try {
			if (tweetsVolume.containsKey(Tweet.parseTweetTimeToDate(tweetobj.created_at))) {
				tweetsVolume.put(Tweet.parseTweetTimeToDate(tweetobj.created_at),
						(tweetsVolume.get(Tweet.parseTweetTimeToDate(tweetobj.created_at)) + 1));
			} else {
				tweetsVolume.put(Tweet.parseTweetTimeToDate(tweetobj.created_at), 1);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public List<Tweet> getTweet(){
		List<Tweet> tweets = new ArrayList<Tweet>();
		while(this.size() > 0){
			tweets.add(this.pop());
		}
		return tweets;
	}

	public List<ActiveUsers> getActiveUser() {
		List<ActiveUsers> activePeopleResult;
		// Active people
		activePeopleResult = new ArrayList<ActiveUsers>(activePeople.values());
		Collections.sort(activePeopleResult);
		this.activePeople.clear();
		return activePeopleResult;
	}

	public List<PopularHashtags> getPopularHashtags() {
		List<PopularHashtags> popularHashtagsResult;
		// popular hashtas
		popularHashtagsResult = new ArrayList<PopularHashtags>(
				popularHashtags.values());
		Collections.sort(popularHashtagsResult);
		this.popularHashtags.clear();
		return popularHashtagsResult;
	}

	public List<PopularUsers> getPopularUser() {
		List<PopularUsers> popularPeopleResult;
		// popular users
		popularPeopleResult = new ArrayList<PopularUsers>(
				popularPeople.values());
		Collections.sort(popularPeopleResult);
		this.popularPeople.clear();
		return popularPeopleResult;
	}
	
	public List<TweetVolumes> getTweetVolume() throws ParseException{
		List<TweetVolumes> result = new ArrayList<TweetVolumes>();
		Iterator it = tweetsVolume.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry obj = (Map.Entry) it.next();
			result.add(new TweetVolumes((Date) obj.getKey(), (Integer) obj
					.getValue()));
		}
		this.tweetsVolume.clear();
		Collections.sort(result);
		return result;
	}

}
