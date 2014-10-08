package org.gistic.taghreed.collections;

/**
 * Created by saifalharthi on 5/29/14.
 */
public class PopularHashtags implements Comparable<PopularHashtags>{

    public String hashtagName;
    public int hashtagCount;

    public PopularHashtags(String hashtagName , int hashtagCount)
    {
        this.hashtagName = hashtagName;
        this.hashtagCount = hashtagCount;
    }
    @Override
    public int compareTo(PopularHashtags o) {
        return o.hashtagCount - hashtagCount;
    }

    @Override
    public String toString() {
        return hashtagName+","+hashtagCount; //To change body of generated methods, choose Tools | Templates.
    }
    
    

}