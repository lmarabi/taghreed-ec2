package org.gistic.taghreed.collections;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.gistic.taghreed.spatialHadoop.Tweets;

//import org.apache.hadoop.util.PriorityQueue;

public class TopTweetResult extends PriorityBlockingQueue<Tweet> {

	// private int capacity;
	private ConcurrentHashMap<String, Integer> popularHashtags = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<String, Integer> activePeople = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<String, Integer> popularPeople = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<String, Integer> tweetsVolume = new ConcurrentHashMap<String, Integer>();
	private Random r;
	private  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public TopTweetResult(int size) {
		super(size);
		r = new Random();
	}

	@Override
	public void put(Tweet element) {
		// boolean overflow = this.size() == this.capacity;
		// int R = r.nextInt(this.capacity - 0) + 0;
		int priorityValue = r.nextInt();
		element.setPriorityValue(priorityValue);
//		setStatistics(element);
		int counts;
		try {
			// Get active people and popular users on the fly
			if (activePeople.containsKey(element.screenName)) {
				activePeople.put(element.screenName, activePeople.get(element.screenName)+1);
				counts = Math.max(popularPeople.get(element.screenName),element.followersCount);
				popularPeople.put(element.screenName, counts);
				
			} else {
				activePeople.put(element.screenName, 1);
				popularPeople.put(element.screenName, element.followersCount);
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}

		// Get popular Hashtags on the fly.
		try {
			List<String> hashtags = new ArrayList<String>();
			String temp = "";
			boolean flag = false;
			if (element.tweetText.contains("#")) {
				for(int index=0 ; index< element.tweetText.length(); index++){
					if(element.tweetText.charAt(index) == '#' && flag == false){
						flag = true; 
					}
					if(element.tweetText.charAt(index) != ' ' && (index+1) != element.tweetText.length()){
						if(flag){
							temp += element.tweetText.charAt(index);
						}
					}else if(flag){
						temp += element.tweetText.charAt(index);
						hashtags.add(temp.replace(" ", ""));
						temp ="";
						flag = false;
					}
				}
				 
				// iterate the list of hashtags 
				for(int i =0 ; i < hashtags.size(); i++){
					if(popularHashtags.contains(hashtags.get(i))){
						counts = popularHashtags.get(hashtags.get(i));
						popularHashtags.put(hashtags.get(i), counts);
						
					}else{
						popularHashtags.put(hashtags.get(i), 1);
					}
				}
//				String[] token = tweetobj.tweetText.split(" ");
//				for (int i = 0; i < token.length; i++) {
//					// Match the hashtags with the regular expression
//					if (token[i].matches("^#[\\p{L}\\p{N}\\p{M}]+")) {
//						if (popularHashtags.containsKey(token[i])) {
//							counts = popularHashtags.get(token[i]);
//							popularHashtags.put(token[i], counts);
//							
//						}else{
//							popularHashtags.put(token[i], 1);
//						}
//					}
//				}
		}
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}

		// Insert tweets volume
		
		if (tweetsVolume.containsKey(element.created_at)) {
			tweetsVolume.put(element.created_at, (tweetsVolume.get(element.created_at) + 1));
		} else {
			tweetsVolume.put(element.created_at, 1);
		}
		super.put(element);
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
	private void setStatistics(Tweet tweetobj) {
		System.out.println("Processing tweetid : "+ tweetobj.tweetID);
		int counts;
		try {
			// Get active people and popular users on the fly
			if (activePeople.containsKey(tweetobj.screenName)) {
				activePeople.put(tweetobj.screenName, activePeople.get(tweetobj.screenName)+1);
				counts = Math.max(popularPeople.get(tweetobj.screenName),tweetobj.followersCount);
				popularPeople.put(tweetobj.screenName, counts);
				
			} else {
				activePeople.put(tweetobj.screenName, 1);
				popularPeople.put(tweetobj.screenName, tweetobj.followersCount);
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}

		// Get popular Hashtags on the fly.
		try {
			List<String> hashtags = new ArrayList<String>();
			String temp = "";
			boolean flag = false;
			if (tweetobj.tweetText.contains("#")) {
				for(int index=0 ; index< tweetobj.tweetText.length(); index++){
					if(tweetobj.tweetText.charAt(index) == '#' && flag == false){
						flag = true; 
					}
					if(tweetobj.tweetText.charAt(index) != ' ' && (index+1) != tweetobj.tweetText.length()){
						if(flag){
							temp += tweetobj.tweetText.charAt(index);
						}
					}else if(flag){
						temp += tweetobj.tweetText.charAt(index);
						hashtags.add(temp.replace(" ", ""));
						temp ="";
						flag = false;
					}
				}
				 
				// iterate the list of hashtags 
				for(int i =0 ; i < hashtags.size(); i++){
					if(popularHashtags.contains(hashtags.get(i))){
						counts = popularHashtags.get(hashtags.get(i));
						popularHashtags.put(hashtags.get(i), counts);
						
					}else{
						popularHashtags.put(hashtags.get(i), 1);
					}
				}
//				String[] token = tweetobj.tweetText.split(" ");
//				for (int i = 0; i < token.length; i++) {
//					// Match the hashtags with the regular expression
//					if (token[i].matches("^#[\\p{L}\\p{N}\\p{M}]+")) {
//						if (popularHashtags.containsKey(token[i])) {
//							counts = popularHashtags.get(token[i]);
//							popularHashtags.put(token[i], counts);
//							
//						}else{
//							popularHashtags.put(token[i], 1);
//						}
//					}
//				}
		}
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}

		// Insert tweets volume
		
		if (tweetsVolume.containsKey(tweetobj.created_at)) {
			tweetsVolume.put(tweetobj.created_at, (tweetsVolume.get(tweetobj.created_at) + 1));
		} else {
			tweetsVolume.put(tweetobj.created_at, 1);
		}

	}

	public List<Tweet> getTweet() {
		List<Tweet> tweets = new ArrayList<Tweet>();
		while (this.size() > 0) {
			tweets.add(this.poll());
		}
		return tweets;
	}

	public List<ActiveUsers> getActiveUser() {
		List<ActiveUsers> activePeopleResult = new ArrayList<ActiveUsers>();
		// Active people
		Iterator it = activePeople.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String,Integer> obj = (Map.Entry) it.next();
			activePeopleResult.add(new ActiveUsers(obj.getKey(), obj.getValue()));
		}
		Collections.sort(activePeopleResult);
		this.activePeople.clear();
		return activePeopleResult;
	}

	public List<PopularHashtags> getPopularHashtags() {
		List<PopularHashtags> popularHashtagsResult = new ArrayList<PopularHashtags>();
		// popular hashtas
		topPopularHashtags topHashtags = new  topPopularHashtags();
		Iterator it = popularHashtags.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String,Integer> obj = (Map.Entry) it.next();
			topHashtags.put(new PopularHashtags(obj.getKey(), obj.getValue()));
		}
//		Collections.sort(popularHashtagsResult);
//		this.popularHashtags.clear();
		while(!topHashtags.isEmpty()){
			popularHashtagsResult.add(topHashtags.poll());
		}
		this.popularHashtags.clear();
		return popularHashtagsResult;
		
	}

	public List<PopularUsers> getPopularUser() {
		List<PopularUsers> popularPeopleResult = new ArrayList<PopularUsers>();
		topPopularUser popularUser = new topPopularUser();
		// popular users
		Iterator it = popularPeople.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String,Integer> obj = (Map.Entry) it.next();
			popularUser.put(new PopularUsers(obj.getKey(), obj.getValue()));
		}
//		Collections.sort(popularPeopleResult);
//		this.popularPeople.clear();
		while(!popularUser.isEmpty()){
			popularPeopleResult.add(popularUser.poll());
		}
		this.popularPeople.clear();
		return popularPeopleResult;
	}

	public List<TweetVolumes> getTweetVolume() throws ParseException {
		List<TweetVolumes> result = new ArrayList<TweetVolumes>();
		Iterator it = tweetsVolume.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String,Integer> obj = (Map.Entry) it.next();
			result.add(new TweetVolumes(obj.getKey(),  obj
					.getValue()));
		}
		this.tweetsVolume.clear();
		Collections.sort(result);
		return result;
	}

	class topPopularUser extends PriorityBlockingQueue<PopularUsers>{
		
		public topPopularUser() {
			super(25);
		}
		@Override
		public void put(PopularUsers arg0) {
			super.put(arg0);
		}
	}
	
	class topActiveUsers extends PriorityBlockingQueue<ActiveUsers>{
		public topActiveUsers() {
			super(25);
		}
		
		@Override
		public void put(ActiveUsers arg0) {
			super.put(arg0);
		}
	}
	
	class topPopularHashtags extends PriorityBlockingQueue<PopularHashtags>{
		public topPopularHashtags() {
			super(50);
		}
		
		@Override
		public void put(PopularHashtags arg0) {
			super.put(arg0);
		}
	}
	
}

