package org.gistic.taghreed.collections;

/**
 * Created by saifalharthi on 5/19/14.
 */
public class PopularUsers implements Comparable<PopularUsers> {

    public String screenName;
    public int followersCount;

    public PopularUsers(String userName, int followerCount) {
        this.screenName = userName;
        this.followersCount = followerCount;
    }

    @Override
    public int compareTo(PopularUsers o) {
        return o.followersCount - followersCount;
    }
    
    @Override
    public String toString() {
    	return this.screenName+","+this.followersCount;
    }
}
