/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;

/**
 *
 * @author louai
 */
public class Test {

    public static void main(String[] arg) throws IOException, FileNotFoundException, ParseException {
        ServerRequest req = new ServerRequest();
        String maxlat = "21.509878763366647";
        String maxlon = "39.206080107128436";
        String minlat = "21.473700876235167";
        String minlon = "39.15913072053149";
        req.setMBR(maxlat, maxlon, minlat, minlon);
        req.setStartDate("2014-01-06");
        req.setEndDate("2014-01-06");
        req.setQuery("اجمل");
        List<Tweet> tweetsResult;
        
        long starttime = System.currentTimeMillis();
        
//        tweetsResult = req.getTweetsInvertedDay();
        tweetsResult = req.getTweetsRtreeDays();
        
        
        long endtime = System.currentTimeMillis();
        System.out.println("*******************************");
        Iterator it = tweetsResult.iterator();
        while(it.hasNext()){
            Tweet t = (Tweet)it.next();
            System.out.println(t.toString());
        }
        System.err.println("Execution time in milliSecond :"+ (endtime-starttime));
        System.out.println(tweetsResult.size());
    }

}
