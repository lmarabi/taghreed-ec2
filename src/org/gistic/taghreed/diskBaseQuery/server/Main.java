/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.gistic.taghreed.collections.PopularHashtags;
import org.gistic.taghreed.collections.Tweet;

/**
 *
 * @author turtle
 */
public class Main {

    public static void main(String[] args)
            throws FileNotFoundException, UnsupportedEncodingException, IOException, ParseException {
        ServerRequest req = new ServerRequest();
        String maxlat = "21.509878763366647";
        String maxlon = "39.206080107128436";
        String minlat = "21.473700876235167";
        String minlon = "39.15913072053149";
        req.setMBR(maxlat, maxlon, minlat, minlon);
        req.setStartDate("2014-01-06");
        req.setEndDate("2014-01-06");
        req.setQuery("سلام");
        String index = "invereted";
        String level = "pyramid";
        List<PopularHashtags> popularHashtags = new ArrayList<PopularHashtags>();
        List<Tweet> tweets = new ArrayList<Tweet>();

        if (level.equals("day")) {
            if (index.equals("rtree")) {
                tweets = req.getTweetsRtreeDays();
                popularHashtags = req.getHashtagsRtreeDays();
            } else {
                tweets = req.getTweetsInvertedDay();
                popularHashtags = req.getHashtagsInvertedDays();
            }

        } else {
            if (index.equals("rtree")) {
                tweets = req.getTweetsRtreePyramid();
                popularHashtags = req.getHashtagsRtreePyramid();
            }else{
                //query from inverted index
                tweets = req.getTweetsInvertedPyramid();
//                popularHashtags = req.getHashtagsInvertedPyramid();
            }

        }

        for (Tweet t : tweets) {
            System.out.println(t.toString());
        }
        for (PopularHashtags hash : popularHashtags) {
            System.out.println(hash.toString());
        }

        System.out.println("Tweets Size:" + tweets.size());
        System.out.println("Hashtags Size = " + popularHashtags.size());

    }

}
