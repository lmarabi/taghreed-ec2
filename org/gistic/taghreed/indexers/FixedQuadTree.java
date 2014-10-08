package org.gistic.taghreed.indexers;

import org.gistic.taghreed.collections.Tweet;

import java.util.List;

/**
 * Created by saifalharthi on 5/25/14.
 */
public class FixedQuadTree {

    private class Node
    {
        Node SE , SW , NE , NW;
        Tweet tweet;

    }

    public FixedQuadTree()
    {

    }

    FixedQuadTree[] quadTreeWeekIndex = new FixedQuadTree[7];

    public void insert(Node node)
    {

    }

    public List<Tweet> query(double maxLat , double maxLong , double minLat , double minLong)

    {
        return null ;
    }


}
