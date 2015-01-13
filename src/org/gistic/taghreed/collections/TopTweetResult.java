package org.gistic.taghreed.collections;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.util.PriorityQueue;

//import org.apache.hadoop.util.PriorityQueue;

public class TopTweetResult extends PriorityQueue<Tweet> {

	// private int capacity;

	private Map<String, Integer> popularHashtags = new ConcurrentHashMap<String, Integer>();
	private Map<String, Integer> activePeople = new ConcurrentHashMap<String, Integer>();
	private Map<String, Integer> tweetsVolume = new ConcurrentHashMap<String, Integer>();

	private topPopularUser topPopularUser = new topPopularUser(20);
	private topPopularHashtags topHashtags = new topPopularHashtags(50);
	private topActiveUsers topActiveUsers = new topActiveUsers(20);

	private Random r;

	public TopTweetResult(int size) {
		super.initialize(size);
		r = new Random();

	}
	
	
	

	@Override
	public boolean insert(Tweet element) {
		
		element.priority = r.nextInt();
		topPopularUser.insert(new PopularUsers(element.screen_name,
				element.follower_count));
		// // setStatistics(element);
		int counts;
		try {
			// Get active people and popular users on the fly
			if (activePeople.containsKey(element.screen_name)) {
				activePeople.put(element.screen_name,
						activePeople.get(element.screen_name) + 1);
				// counts =
				// Math.max(popularPeople.get(element.screen_name),element.follower_count);
				// popularPeople.put(element.screen_name, counts);

			} else {
				activePeople.put(element.screen_name, 1);
				// popularPeople.put(element.screen_name,
				// element.follower_count);
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}
		//
		// Get popular Hashtags on the fly.
		try {
			List<String> hashtags = new ArrayList<String>();
			String temp = "";
			boolean flag = false;
			if (element.tweet_text.contains("#")) {
				for (int index = 0; index < element.tweet_text.length(); index++) {
					if (element.tweet_text.charAt(index) == '#'
							&& flag == false) {
						flag = true;
					}
					if (element.tweet_text.charAt(index) != ' '
							&& (index + 1) != element.tweet_text.length()) {
						if (flag /* && element.tweet_text.charAt(index) != '#' */) {
							temp += element.tweet_text.charAt(index);
						}
					} else if (flag) {
						temp += element.tweet_text.charAt(index);
						hashtags.add(temp.replace(" ", ""));
						temp = "";
						flag = false;
					}
				}

				// iterate the list of hashtags
				for (int i = 0; i < hashtags.size(); i++) {
					if(popularHashtags.containsKey(hashtags.get(i))){
						popularHashtags.put(hashtags.get(i), (popularHashtags.get(hashtags.get(i))+1));
					}else{
						popularHashtags.put(hashtags.get(i), 1);
					}
				}

			}
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}

		// Insert tweets volume
		String time = element.created_at.substring(0, 9);
		if (tweetsVolume.containsKey(time)) {
			tweetsVolume.put(time,
					(tweetsVolume.get(time) + 1));
		} else {
			tweetsVolume.put(time, 1);
		}
		
		return super.insert(element);
		

	}

	protected boolean lessThan(Object arg0, Object arg1) {
		return ((Tweet) arg0).getPriorityValue() >= ((Tweet) arg1)
				.getPriorityValue();
	}

	/**
	 * This method set the active people and popular people and hashtags on the
	 * fly.
	 * 
	 * @param tweetobj
	 * @throws ParseException
	 */
	/*
	 * private void setStatistics(Tweet tweetobj) {
	 * System.out.println("Processing tweetid : "+ tweetobj.tweetID); int
	 * counts; try { // Get active people and popular users on the fly if
	 * (activePeople.containsKey(tweetobj.screenName)) {
	 * activePeople.put(tweetobj.screenName,
	 * activePeople.get(tweetobj.screenName)+1); counts =
	 * Math.max(popularPeople.get(tweetobj.screenName),tweetobj.followersCount);
	 * popularPeople.put(tweetobj.screenName, counts);
	 * 
	 * } else { activePeople.put(tweetobj.screenName, 1);
	 * popularPeople.put(tweetobj.screenName, tweetobj.followersCount); } }
	 * catch (ArrayIndexOutOfBoundsException e) { e.printStackTrace(); }
	 * 
	 * // Get popular Hashtags on the fly. try { List<String> hashtags = new
	 * ArrayList<String>(); String temp = ""; boolean flag = false; if
	 * (tweetobj.tweetText.contains("#")) { for(int index=0 ; index<
	 * tweetobj.tweetText.length(); index++){
	 * if(tweetobj.tweetText.charAt(index) == '#' && flag == false){ flag =
	 * true; } if(tweetobj.tweetText.charAt(index) != ' ' && (index+1) !=
	 * tweetobj.tweetText.length()){ if(flag){ temp +=
	 * tweetobj.tweetText.charAt(index); } }else if(flag){ temp +=
	 * tweetobj.tweetText.charAt(index); hashtags.add(temp.replace(" ", ""));
	 * temp =""; flag = false; } }
	 * 
	 * // iterate the list of hashtags for(int i =0 ; i < hashtags.size(); i++){
	 * if(popularHashtags.contains(hashtags.get(i))){ counts =
	 * popularHashtags.get(hashtags.get(i));
	 * popularHashtags.put(hashtags.get(i), counts);
	 * 
	 * }else{ popularHashtags.put(hashtags.get(i), 1); } } // String[] token =
	 * tweetobj.tweetText.split(" "); // for (int i = 0; i < token.length; i++)
	 * { // // Match the hashtags with the regular expression // if
	 * (token[i].matches("^#[\\p{L}\\p{N}\\p{M}]+")) { // if
	 * (popularHashtags.containsKey(token[i])) { // counts =
	 * popularHashtags.get(token[i]); // popularHashtags.put(token[i], counts);
	 * // // }else{ // popularHashtags.put(token[i], 1); // } // } // } } }
	 * catch (ArrayIndexOutOfBoundsException e) { e.printStackTrace(); }
	 * 
	 * // Insert tweets volume
	 * 
	 * if (tweetsVolume.containsKey(tweetobj.created_at)) {
	 * tweetsVolume.put(tweetobj.created_at,
	 * (tweetsVolume.get(tweetobj.created_at) + 1)); } else {
	 * tweetsVolume.put(tweetobj.created_at, 1); }
	 * 
	 * }
	 */
	public List<Tweet> getTweet() {
		List<Tweet> tweets = new ArrayList<Tweet>();
		while (this.size() > 0) {
			tweets.add(this.pop());
		}
		this.clear();
		return tweets;
	}

	public List<ActiveUsers> getActiveUser() {
		List<ActiveUsers> result = new ArrayList<ActiveUsers>();
		Iterator it = activePeople.entrySet().iterator();
		while(it.hasNext()){
			Entry<String, Integer> obj = (Entry<String, Integer>) it.next();
			topActiveUsers.insert(new ActiveUsers(obj.getKey(), obj.getValue()));
		}
		while(topActiveUsers.size() > 0){
			result.add(topActiveUsers.pop());
		}
		activePeople.clear();
//		topActiveUsers.clear();
//		Collections.sort(result);
		return result;
	}

	public List<PopularHashtags> getPopularHashtags() {
		List<PopularHashtags> result = new ArrayList<PopularHashtags>();
		Iterator it = popularHashtags.entrySet().iterator();
		while(it.hasNext()){
			Entry<String, Integer> obj = (Entry<String, Integer>) it.next();
			topHashtags.insert(new PopularHashtags(obj.getKey(), obj.getValue()));
		}
		while(topHashtags.size() > 0){
			result.add(topHashtags.pop());
		}
//		topHashtags.clear();
		popularHashtags.clear();
//		Collections.sort(result);
		return result;

	}

	public List<PopularUsers> getPopularUser() {
		List<PopularUsers> result = new ArrayList<PopularUsers>();
		while(topPopularUser.size() > 0){
			result.add(topPopularUser.pop());
		}
		topPopularUser.clear();
//		Collections.sort(result);
		return result;
	}

	public List<TweetVolumes> getTweetVolume() throws ParseException {
		List<TweetVolumes> result = new ArrayList<TweetVolumes>();
		Iterator it = tweetsVolume.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Integer> obj = (Map.Entry) it.next();
			result.add(new TweetVolumes(obj.getKey(), obj.getValue()));
		}
		this.tweetsVolume.clear();
//		tweetsVolume.clear();
//		Collections.sort(result);
		return result;
	}

	class topPopularUser extends PriorityQueue<PopularUsers> {

		public topPopularUser(int size) {
			super.initialize(size);
		}

		@Override
		public boolean insert(PopularUsers arg0) {
			return super.insert(arg0);
		}

		@Override
		protected boolean lessThan(Object arg0, Object arg1) {
			return ((PopularUsers) arg0).followersCount <=  ((PopularUsers) arg1).followersCount;
		}
		
	}

	class topActiveUsers extends PriorityQueue<ActiveUsers> {
		public topActiveUsers(int size) {
			super.initialize(size);
		}

		@Override
		public boolean insert(ActiveUsers arg0) {
			return super.insert(arg0);

		}

		@Override
		protected boolean lessThan(Object arg0, Object arg1) {
			return ((ActiveUsers) arg0).tweetCount >= ((ActiveUsers) arg1).tweetCount;
		}
	}

	class topPopularHashtags extends PriorityQueue<PopularHashtags> {
		public topPopularHashtags(int size) {
			super.initialize(size);
		}

		@Override
		public boolean insert(PopularHashtags element) {
			// TODO Auto-generated method stub
			return super.insert(element);
		}
		

		@Override
		protected boolean lessThan(Object arg0, Object arg1) {
			return ((PopularHashtags) arg0).hashtagCount >= ((PopularHashtags) arg1).hashtagCount;
		}
	}

}