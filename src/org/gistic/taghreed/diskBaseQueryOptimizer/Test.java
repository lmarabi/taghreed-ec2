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
        ServerRequest req = new ServerRequest(1);
        String maxlat = "90";
        String maxlon = "180";
        String minlat = "-90";
        String minlon = "-180";
        req.setMBR(maxlat, maxlon, minlat, minlon);
        req.setStartDate("2014-05-01");
        req.setEndDate("2014-05-30");
//        req.setQuery("اجمل");
//        List<Tweet> tweetsResult;
//        
//        long starttime = System.currentTimeMillis();
        
//        tweetsResult = req.getTweetsInvertedDay();
        
//        
//        long endtime = System.currentTimeMillis();
//        System.out.println("*******************************");
//        Iterator it = tweetsResult.iterator();
//        while(it.hasNext()){
//            Tweet t = (Tweet)it.next();
//            System.out.println(t.toString());
//        }
//        System.err.println("Execution time in milliSecond :"+ (endtime-starttime));
//        System.out.println(tweetsResult.size());
    }

}
