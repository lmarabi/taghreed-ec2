/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.query;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gistic.taghreed.collections.TweetVolumes;
import org.gistic.taghreed.collections.Week;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQueryOptimizer.GridCell;

/**
 *
 * @author turtle
 */
public class Queryoptimizer {
    //Global lookup

    public static Lookup lookup = new Lookup();
    public static PyramidQueryProcessor pyramidRequest;
    public static DayQueryProcessor dayrequest;
    public static List<TweetVolumes> dayVolume;
    private static ServerRequest serverRequest;
    private static String[] headWeeks;

    public Queryoptimizer(ServerRequest serverRequest) throws IOException, FileNotFoundException, ParseException {
        this.serverRequest = serverRequest;
        pyramidRequest = new PyramidQueryProcessor(serverRequest);
        dayrequest = new DayQueryProcessor(serverRequest);
        //Load lookuptabe
        if(serverRequest.getIndex().equals(ServerRequest.queryIndex.rtree)){
            lookup.loadLookupTableToArrayList(this.serverRequest.getRtreeDir());
        }else{
            lookup.loadLookupTableToArrayList(this.serverRequest.getInvertedDir());
        }

    }

    /**
     * @param args the command line arguments
     */
    public List<TweetVolumes> executeQuery() throws FileNotFoundException,
            UnsupportedEncodingException, IOException, ParseException {

        boolean queryTail = false;
        double startTime = System.currentTimeMillis();
        dayrequest.createStreamer();
//        pyramidRequest.createStreamer();
        //Query From Months
        Map<String, String> indexMonths = getMonthTree();
        System.out.println("#number of Months found: " + indexMonths.size());
        Iterator it = indexMonths.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            System.out.println("#Start Reading index of " + entry.getKey().toString());
            dayrequest.GetSmartOutput(entry.getKey().toString(),entry.getValue().toString());
        }
        //Copy TweetVolume and check weeks
        pyramidRequest.setWeekVolume(dayrequest.getTweetsVolume());
        pyramidRequest.parseStreamer(dayrequest.getStreamer());
        //Check non Queried week in previous month query.
        if (indexMonths.size() > 0) {
            headWeeks = lookup.getHeadofSubMonth(serverRequest.getStartDate(),
                    serverRequest.getEndDate());
            queryTail = true;
        } else {
            headWeeks = new String[2];
            headWeeks[0] = serverRequest.getStartDate();
            headWeeks[1] = serverRequest.getEndDate();
            queryTail = true;
        }
        if (headWeeks[0] != null && headWeeks[1] != null) {
            Map<Week, String> index = getWeekTree(headWeeks[0], headWeeks[1]);
            System.out.println("#number of dates found: " + index.size());
            it = index.entrySet().iterator(); 
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                Week week = (Week) entry.getKey();
                System.out.println("#Start Reading index of " + week.getStart() + "-" + week.getEnd());
                pyramidRequest.GetSmartOutput(entry.getKey().toString(),entry.getValue().toString());
            }
            //Query missing days in head week
            pyramidRequest.parseStreamer(dayrequest.getStreamer());
            Map<Date, String> indexDays = getDayTree(headWeeks[0], headWeeks[1]);
            it = indexDays.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                System.out.println("#Start Reading index of " + entry.getKey().toString());
                dayrequest.GetSmartOutput(entry.getKey().toString(),entry.getValue().toString());
            }
        } else {
//                //If there is no week and only days
//                pyramidRequest.parseStreamer(dayrequest.getStreamer());
//                Map<Date, String> indexDays = getDayTree(args[8], headWeeks[0], headWeeks[1]);
//                it = indexDays.entrySet().iterator();
//                while (it.hasNext()) {
//                    Map.Entry entry = (Map.Entry) it.next();
//                    System.out.println("#Start Reading index of " + entry.getKey().toString());
//                    int count = dayrequest.GetSmartOutput(args[2], args[3], args[4], args[5], entry.getValue().toString() + "/");
//                    Date date = (Date) entry.getKey();
//                    pyramidRequest.addTweetVolume(date, count);
//                }
        }
        //Query Tail week
        if (queryTail) {
            String[] tailWeeks = lookup.getTailofSubMonth(serverRequest.getStartDate(),
                    serverRequest.getEndDate());
            if (tailWeeks[0] != null && tailWeeks[1] != null) {
                Map<Week, String> index = getWeekTree(tailWeeks[0], tailWeeks[1]);
                System.out.println("#number of dates found: " + index.size());
                it = index.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry entry = (Map.Entry) it.next();
                    Week week = (Week) entry.getKey();
                    System.out.println("#Start Reading index of " + week.getStart() + "-" + week.getEnd());
                    pyramidRequest.GetSmartOutput(entry.getKey().toString(),entry.getValue().toString());
                }
                //Query missing days in head week
                pyramidRequest.parseStreamer(dayrequest.getStreamer());
                Map<Date, String> indexDays = getDayTree(tailWeeks[0], tailWeeks[1]);
                it = indexDays.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry entry = (Map.Entry) it.next();
                    System.out.println("#Start Reading index of " + entry.getKey().toString());
                    dayrequest.GetSmartOutput(entry.getKey().toString(),entry.getValue().toString());
                }
            }
        }

        double endTime = System.currentTimeMillis();
        System.out.println("query time = " + (endTime - startTime) + " ms");
        dayrequest.closeStreamer();
        pyramidRequest.closeStreamer();
        return pyramidRequest.getTweetVolume();

    }
    
    /***
     * This method read the master file and return the grid cells 
     * @return
     * @throws ParseException 
     * @throws IOException 
     * @throws UnsupportedEncodingException 
     * @throws FileNotFoundException 
     */
    public GridCell readMastersFile(queryLevel level) throws FileNotFoundException, UnsupportedEncodingException, IOException, ParseException{
    	GridCell cell = new GridCell(this.serverRequest.getMbr());
    	
    	if(level.equals(queryLevel.Week)){
    		Map<Week, String> index = getWeekTree(this.serverRequest.getStartDate(),this.serverRequest.getEndDate());
    		Iterator it = index.entrySet().iterator(); 
            while (it.hasNext()) {
                Map.Entry<Week,String> entry = (Map.Entry) it.next();
                Week week = entry.getKey();
                System.out.println("#Start Reading index of " + week.getStart() + "-" + week.getEnd());
                cell = pyramidRequest.readMasterFile(entry.getKey().toString(),entry.getValue().toString());
            }
    	}else{
    		 Map<String, String> indexMonths = getMonthTree();
    	        System.out.println("#number of Months found: " + indexMonths.size());
    	        Iterator it = indexMonths.entrySet().iterator();
    	        while (it.hasNext()) {
    	            Map.Entry entry = (Map.Entry) it.next();
    	            System.out.println("#Start Reading index of " + entry.getKey().toString());
    	            cell = pyramidRequest.readMasterFile(entry.getKey().toString(),entry.getValue().toString());
    	        }
    	}
    	
    	 
    	return cell;
    }

    /**
     * This method check if the dayrequest for tweets or hashtags
     *
     * @param type
     * @param start
     * @param end
     * @return
     * @throws ParseException
     */
    private static Map<Week, String> getWeekTree(String start, String end) throws ParseException {
        if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
            return lookup.getTweetsWeekIndex(start, end);
        } else {
            PyramidQueryProcessor.queryType = PyramidQueryProcessor.QueryRequest.hashtag;
            return lookup.getHashtagWeekIndex(start, end);
        }
    }

    private static Map<Date, String> getDayTree(String start, String end) throws ParseException {
        if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
            return lookup.getTweetMissingDaysinWeek(start, end);
        } else {
            return lookup.getHashtagMissingDaysinWeek(start, end);
        }
    }

    private static Map<String, String> getMonthTree() throws ParseException, IOException {
        if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
            return lookup.getTweetsMonthsIndex(serverRequest.getStartDate(),
                    serverRequest.getEndDate());
        } else {
            return lookup.getHashtagMonthsIndex(serverRequest.getStartDate(),
                    serverRequest.getEndDate());
        }
    }

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException, IOException, ParseException {
//        args = new String[11];
//        args[0] = "/home/turtle/UQUGIS/taghreed/Tools/twittercrawlermavenproject/output/result/";//System.getProperty("user.dir") + "/data/result";
//        args[1] = System.getProperty("user.dir") + "/export/";
//        args[2] = "180";//"21.509878763366647";//jedah
//        args[3] = "180";//"39.206080107128436";
//        args[4] = "-180";//"21.473700876235167";
//        args[5] = "-180";//"39.15913072053149";
//        args[6] = "2013-10-01";
//        args[7] = "2013-12-01";
//        args[8] = "tweet";
//        args[9] = "multi";
//        args[10] = "";
//        String folderPath = args[0] + "/";
//        lookup.loadLookupTableToArrayList(folderPath);
//        List<TweetVolumes> result = org.gistic.taghreed.diskBaseQuery.query.Queryoptimizer.run(args);
//        Collections.sort(result);
//        for (int i = 0; i < result.size(); i++) {
//            System.out.println(result.get(i).dayName + "-" + result.get(i).volume);
//        }
    }
}
