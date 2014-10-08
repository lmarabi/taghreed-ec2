package org.gistic.taghreed.search;

import org.gistic.taghreed.collections.*;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Created by saifalharthi on 6/12/14.
 */
public class Results {

    HashMap<String , ActiveUsers> activeUsersHashMap = null;

    HashMap<String , PopularUsers> popularUsersHashMap = null;

    HashMap<String , PopularHashtags> popularHashtagsHashMap = null;

    HashMap<String , TweetVolumes> tweetVolumesHashMap = null;

    public List<ActiveUsers> getActiveUsers(List<Tweet> tweets)
    {
        System.out.println("Entered Active Users fucntion");
            activeUsersHashMap = new HashMap<String, ActiveUsers>();
            for (Tweet tweet : tweets) {

                if (activeUsersHashMap.containsKey(tweet.userID))
                    activeUsersHashMap.get(tweet.screenName).tweetCount++;
                else
                    activeUsersHashMap.put(tweet.screenName, new ActiveUsers(tweet.screenName, 1));
            }
            System.out.println("Computed Active Users.....");

        List<ActiveUsers> activeUsers = new ArrayList<ActiveUsers>(activeUsersHashMap.values());
        Collections.sort(activeUsers);
        activeUsersHashMap.clear();
        return activeUsers ;

    }

    public List<PopularUsers> getPopularUsers(List<Tweet> tweets)
    {
        System.out.println("Entered Popular Users fucntion");

            popularUsersHashMap = new HashMap<String, PopularUsers>();
            for (Tweet tweet : tweets) {
                if (popularUsersHashMap.containsKey(tweet.screenName))
                    popularUsersHashMap.get(tweet.screenName).followersCount = Math.max(tweet.followersCount, popularUsersHashMap.get(tweet.screenName).followersCount);
                else
                    popularUsersHashMap.put(tweet.screenName, new PopularUsers(tweet.screenName, tweet.followersCount));
            }
            System.out.println("Computed Popular Users.....");

        List<PopularUsers> popularUsers = new ArrayList<PopularUsers>(popularUsersHashMap.values());
        Collections.sort(popularUsers);

        popularUsersHashMap.clear();
        return popularUsers;
    }

    public List<PopularHashtags> getPopularHashtags(List<Tweet> tweets)
    {
        popularHashtagsHashMap = new HashMap<String, PopularHashtags>();
        return null;
    }

    public List<TweetVolumes> getTweetVolumes(List<Tweet> tweets){

        System.out.println("Entered Tweet Volume Function");

            tweetVolumesHashMap = new HashMap<String, TweetVolumes>();

            for (Tweet tweet : tweets) {
                String[] timeStampSplit = tweet.created_at.split(" ");
                if (tweetVolumesHashMap.containsKey(timeStampSplit[0]))
                    tweetVolumesHashMap.get(timeStampSplit[0]).volume++;
                else
                    tweetVolumesHashMap.put(timeStampSplit[0], new TweetVolumes(timeStampSplit[0], 1));
            }
            System.out.println("Computed Tweets per day.....");

        List<TweetVolumes> tweetVolumeses = new ArrayList<TweetVolumes>(tweetVolumesHashMap.values());
        Collections.sort(tweetVolumeses);
        tweetVolumesHashMap.clear();
        return tweetVolumeses;
    }

    }


