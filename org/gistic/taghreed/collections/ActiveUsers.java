package org.gistic.taghreed.collections;

/**
 * Created by saifalharthi on 5/19/14.
 */
public class ActiveUsers implements Comparable<ActiveUsers>{

    public String userName;
    public int tweetCount;

    public ActiveUsers(String userName, int tweetCount) {
        this.userName = userName;
        this.tweetCount = tweetCount;
    }

    @Override
    public int compareTo(ActiveUsers o) {
        return o.tweetCount - tweetCount;
    }
}
