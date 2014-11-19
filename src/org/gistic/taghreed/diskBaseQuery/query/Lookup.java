/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.query;

import org.gistic.taghreed.collections.Week;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Months;

/**
 *
 * @author turtle
 */
public class Lookup {

    private List<Date> dayDatesTweet = new ArrayList<Date>();
    private List<String> monthDatesTweet = new ArrayList<String>();
    private List<String> monthPathsTweet = new ArrayList<String>();
    private List<String> monthDatesHashtag = new ArrayList<String>();
    private List<String> monthPathsHashtag = new ArrayList<String>();
    private List<String> dayPathsTweet = new ArrayList<String>();
    private List<Date> dayDatesHashtag = new ArrayList<Date>();
    private List<String> dayPathsHashtag = new ArrayList<String>();
    private List<Week> weekDatesTweet = new ArrayList<Week>();
    private List<String> weekPathsTweet = new ArrayList<String>();
    private List<Week> weekDatesHashtag = new ArrayList<Week>();
    private List<String> weekPathsHashtag = new ArrayList<String>();
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private Map<String, String> dayLookupTweet = new HashMap<String, String>();
    private Map<String, String> dayLookupHashtag = new HashMap<String, String>();
    private Map<String, String> weekLookupTweet = new HashMap<String, String>();
    private Map<String, String> weekLookupHashtag = new HashMap<String, String>();
    private List<Date> missingDays = new ArrayList<Date>();

    public Lookup() {
    }

    /**
     * This method will load the lookupTable to the memory
     *
     * @param path
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void loadLookupTableToArrayList(String path) throws FileNotFoundException, IOException, ParseException {
    	//************************ Load missing days
    	String missing_Day_file = path + "/tweets/Day/_missing_Days.txt";
        System.out.println("Load missing days into memory");
        if(new File(missing_Day_file).exists()){
        	BufferedReader reader = new BufferedReader(new FileReader(missing_Day_file));
            String line = null;
            while ((line = reader.readLine()) != null) {
            	missingDays.add(dateFormat.parse(line));

            }
            reader.close();
        }
    	//************************ Day lookup tables
    	String tweetsLookup = path + "/tweets/Day/lookupTable.txt";
        System.out.println("Load lookup tables into memory");
        BufferedReader reader = new BufferedReader(new FileReader(tweetsLookup));
        String line = null;
        while ((line = reader.readLine()) != null) {
            dayDatesTweet.add(dateFormat.parse(line));
            dayPathsTweet.add(path + "/tweets/Day/index."+line);

        }
        reader.close();

        //************************ Week lookup tables
        tweetsLookup = path + "/tweets/Week/lookupTable.txt";
        reader = new BufferedReader(new FileReader(tweetsLookup));
        line = null;
        while ((line = reader.readLine()) != null) {
            String[] temp = line.split(",");
            String[] range = temp[0].split("&");
            weekDatesTweet.add(new Week(dateFormat.parse(range[0]),
                    dateFormat.parse(range[1])));
            weekPathsTweet.add(path + "/tweets/Week/index."+line);

        }
        reader.close();

        //********** Load lookup for Months 
        tweetsLookup = path + "/tweets/Month/lookupTable.txt";
        reader = new BufferedReader(new FileReader(tweetsLookup));
        line = null;
        while ((line = reader.readLine()) != null) {
            monthDatesTweet.add(line);
            monthPathsTweet.add(path + "/tweets/Month/index."+line);

        }
        reader.close();


    }

    /**
     * This method will load the lookupTable to the memory
     *
     * @param path
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void loadLookupTableHashMap(String path) throws FileNotFoundException, IOException {
        String tweetsLookup = path + "/tweets/lookupTable.txt";
        String hashtagLookup = path + "/hashtags/lookupTable.txt";
        BufferedReader reader = new BufferedReader(new FileReader(tweetsLookup));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] temp = line.split(",");
            dayLookupTweet.put(temp[0], temp[1]);
        }
        reader.close();

        // Read The lookupTable for hashtags
        reader = new BufferedReader(new FileReader(hashtagLookup));
        line = null;
        while ((line = reader.readLine()) != null) {
            String[] temp = line.split(",");
            dayLookupHashtag.put(temp[0], temp[1]);
        }
        reader.close();
    }

    /**
     * Print the content of the lookupTables
     *
     * @param tweet
     * @param hashtag
     */
    public void printlookUp(boolean tweet, boolean hashtag) {
        if (tweet) {
            Iterator iterator = dayLookupTweet.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry obj = (Map.Entry) iterator.next();
                System.out.println(obj.getKey().toString() + " , " + obj.getValue());
            }
        }
        if (hashtag) {
            Iterator iterator = dayLookupHashtag.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry obj = (Map.Entry) iterator.next();
                System.out.println(obj.getKey().toString() + " , " + obj.getValue());
            }
        }
    }

    /**
     * Print LookupTable
     */
    public void PrintLookupArrayList() {
        System.out.println("Date-Tweet");
        for (Iterator<Date> it = dayDatesTweet.iterator(); it.hasNext();) {
            Date d = it.next();
            System.out.println(d.toString());
        }
        System.out.println("Date-Hashtag");
        for (Iterator<Date> it = dayDatesHashtag.iterator(); it.hasNext();) {
            Date d = it.next();
            System.out.println(d.toString());
        }
    }

    /**
     * This method return HashMap<Date,Path> to all dates between the start and
     * the end date.
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public Map<Date, String> getTweetsDayIndex(String startDate, String endDate)
            throws ParseException {
        Map<Date, String> result = new HashMap<Date, String>();
        Date start = dateFormat.parse(startDate);
        Date end = dateFormat.parse(endDate);
        for (int i = 0; i < dayDatesTweet.size(); i++) {
            if (insideDaysBoundry(start, end, dayDatesTweet.get(i))) {
                result.put(dayDatesTweet.get(i), dayPathsTweet.get(i));
            }
        }
        return result;
    }

    /**
     * This method return HashMap<Date,Path> to all dates between the start and
     * the end date.
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public Map<Date, String> getHashtagDayIndex(String startDate, String endDate)
            throws ParseException {
        Map<Date, String> result = new HashMap<Date, String>();
        Date start = dateFormat.parse(startDate);
        Date end = dateFormat.parse(endDate);
        for (int i = 0; i < dayDatesHashtag.size(); i++) {
            if (insideDaysBoundry(start, end, dayDatesHashtag.get(i))) {
                result.put(dayDatesHashtag.get(i), dayPathsHashtag.get(i));
            }
        }
        return result;
    }

    /**
     * This method return HashMap<Date,Path> to all dates between the start and
     * the end date.
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public Map<Week, String> getTweetsWeekIndex(String startDate, String endDate)
            throws ParseException {
        Map<Week, String> result = new HashMap<Week, String>();
        Date start = dateFormat.parse(startDate);
        Date end = dateFormat.parse(endDate);
        for (int i = 0; i < weekDatesTweet.size(); i++) {
            if (insideWeekBoundry(start, end, weekDatesTweet.get(i))) {
                result.put(weekDatesTweet.get(i), weekPathsTweet.get(i));
            }
        }
        return result;
    }

    /**
     * This method return Full months between two dates
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public Map<String, String> getTweetsMonthsIndex(String startDate, String endDate)
            throws ParseException {
        Map<String, String> result = new HashMap<String, String>();
        Date start = dateFormat.parse(startDate);
        Date end = dateFormat.parse(endDate);
        Calendar c = Calendar.getInstance();
        c.setTime(start);
        List<String> months = new ArrayList<String>();
        while (!start.equals(end)) {
            if (start.getMonth() != end.getMonth()) {
                if (start.getDate() == 1) {
                    //System.out.println("Month found" + (start.getMonth() + 1));
                    String[] temp = Week.dateFormat.format(start).split("-");
                    //add only month to temp
                    months.add(temp[0] + "-" + temp[1]);
                }
            }
            c.add(Calendar.DATE, 1);
            start = c.getTime();
        }

        for (int j = 0; j < months.size(); j++) {
            for (int i = 0; i < monthDatesTweet.size(); i++) {
                if (months.get(j).equals(monthDatesTweet.get(i))) {
                    result.put(months.get(j), monthPathsTweet.get(i));
                }
            }
        }
        return result;
    }

    /**
     * This method return full months between two dates
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public Map<String, String> getHashtagMonthsIndex(String startDate, String endDate)
            throws ParseException {
        Map<String, String> result = new HashMap<String, String>();
        Date start = dateFormat.parse(startDate);
        Date end = dateFormat.parse(endDate);
        Calendar c = Calendar.getInstance();
        c.setTime(start);
        List<String> months = new ArrayList<String>();
        while (!start.equals(end)) {
            if (start.getMonth() != end.getMonth()) {
                if (start.getDate() == 1) {
                    //System.out.println("Month found" + (start.getMonth() + 1));
                    String[] temp = Week.dateFormat.format(start).split("-");
                    //add only month to temp
                    months.add(temp[0] + "-" + temp[1]);
                }
            }
            c.add(Calendar.DATE, 1);
            start = c.getTime();
        }

        for (int j = 0; j < months.size(); j++) {
            for (int i = 0; i < monthDatesHashtag.size(); i++) {
                if (months.get(j).equals(monthDatesHashtag.get(i))) {
                    result.put(months.get(j), monthPathsHashtag.get(i));
                }
            }
        }
        return result;
    }

    /**
     * This method return startdate and endDate of a begining range For example
     * 2014-05-13 to 2014-08-01 then Method will return String[] range =
     * {2014-05-13,2014-05-31}
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public String[] getHeadofSubMonth(String startDate, String endDate) throws ParseException {
        Date start = dateFormat.parse(startDate);
        Date end = dateFormat.parse(endDate);
        Calendar c = Calendar.getInstance();
        String[] result = new String[2];
        List<Date> queried = new ArrayList<Date>();
        //Query for months 
        Map<String,String> queriedM = this.getTweetsMonthsIndex(startDate, endDate);
        Iterator it = queriedM.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry obj = (Map.Entry) it.next();
            queried.add(dateFormat.parse(new String(obj.getKey()+"-1")));
        }
        // Check if the head of the month already queried in months then return null
        if (queried.size() > 0) {
            Collections.sort(queried);
            Date headMonths = queried.get(0);
            if (headMonths.equals(start)) {
                return result;
            }
        }
        // IF the head did not queried in months 
        c.setTime(start);
        String sRange = null;
        String eRange = null;
        List<String> months = new ArrayList<String>();
        while (!start.equals(end)) {
            if (start.getMonth() != end.getMonth()) {
                if (start.getDate() == 1) {
//                    System.out.println("Month found"+(start.getMonth()+1));
                    String[] temp = Week.dateFormat.format(start).split("-");
                    //add only month to temp
                    months.add(temp[0] + "-" + temp[1]);
                    //add the previous range
                    if (sRange != null) {
                        c.add(Calendar.DATE, -1);
                        Date erangeDate = c.getTime();
                        eRange = Week.dateFormat.format(erangeDate);
                        result[1] = eRange;
                        return result;
                    }

                } else {
                    if (sRange == null) {
                        sRange = Week.dateFormat.format(start);
                        result[0] = sRange;
                    }
                }
            }
            c.add(Calendar.DATE, 1);
            start = c.getTime();
        }
        return result;
    }

    /**
     * This method return startdate and endDate of a begining range For example
     * 2014-05-13 to 2014-08-01 then Method will return String[] range =
     * {2014-05-13,2014-05-31}
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public String[] getTailofSubMonth(String startDate, String endDate) throws ParseException {
        String[] result = new String[2];
        List<Date> queried = new ArrayList<Date>();
        Date start = dateFormat.parse(startDate);
        Date end = dateFormat.parse(endDate);
        Calendar c = Calendar.getInstance();
        c.setTime(start);
        String sRange = null;
        String eRange = null;
        List<String> months = new ArrayList<String>();
        //Query for months 
        Map<String,String> queriedM = this.getTweetsMonthsIndex(startDate, endDate);
        Iterator it = queriedM.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry obj = (Map.Entry) it.next();
            Date firstDayofMonth = dateFormat.parse(new String(obj.getKey()+"-1"));
            c.setTime(firstDayofMonth);
            c.add(Calendar.MONTH, 1);
            c.set(Calendar.DAY_OF_MONTH, 1);
            c.add(Calendar.DATE, -1);
            Date lastDay = c.getTime();
            queried.add(lastDay);
        }
        Collections.sort(queried);
        if(queried.size() > 0){
            Date tailDate = queried.get(queried.size() - 1);
            if (!tailDate.equals(end)) {
                c.setTime(tailDate);
                c.add(Calendar.DATE, 1);
                result[0] = dateFormat.format(c.getTime());
                result[1] = dateFormat.format(end);
                return result;
            }
        }
        while (!start.equals(end)) {
                if (start.getDate() == 1) {
//                    System.out.println("Month found"+(start.getMonth()+1));
                    String[] temp = Week.dateFormat.format(start).split("-");
                    //add only month to temp
                    months.add(temp[0] + "-" + temp[1]);
                    //add the previous range
                    Date tempdate = c.getTime();
                    sRange = Week.dateFormat.format(tempdate);
                    result[0] = sRange;


                }
            c.add(Calendar.DATE, 1);
            start = c.getTime();
        }
        eRange = Week.dateFormat.format(start);
        result[1] = eRange;
        if(Week.dateFormat.parse(result[1]).getMonth() == start.getMonth()){
            result = new String[2];
            return result;
        }else{
            return result;
        }
    }

    /**
     * Get the missing tweets lookup Map<Date,String>
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public Map<Date, String> getTweetMissingDaysinWeek(String startDate, String endDate) throws ParseException {
        Map<Date, String> result = new HashMap<Date, String>();
        Date start = dateFormat.parse(startDate);
        Date end = dateFormat.parse(endDate);
        Map<Week, String> weekcovred = this.getTweetsWeekIndex(startDate, endDate);
        Map<Date, String> days = this.getTweetsDayIndex(startDate, endDate);
        Calendar c = Calendar.getInstance();
        boolean cover = false;
        while (!start.after(end)) {
            Iterator it = weekcovred.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry obj = (Map.Entry) it.next();
                Week week = (Week) obj.getKey();
                //check and add to the list
                if (week.isDayIntheWeek(start)) {
                    cover = true;
                }

            }
            if (!cover) {
                if (days.get(start) != null) {
                    result.put(start, days.get(start));
                }
            } else {
                cover = false;
            }
            c.setTime(start);
            c.add(Calendar.DATE, 1);  // number of days to add
            start = c.getTime();
        }
        return result;
    }

    public Map<Date, String> getHashtagMissingDaysinWeek(String startDate, String endDate) throws ParseException {
        Map<Date, String> result = new HashMap<Date, String>();
        Date start = dateFormat.parse(startDate);
        Date end = dateFormat.parse(endDate);
        Map<Week, String> weekcovred = this.getHashtagWeekIndex(startDate, endDate);
        Map<Date, String> days = this.getHashtagDayIndex(startDate, endDate);
        Calendar c = Calendar.getInstance();
        boolean cover = false;
        while (!start.after(end)) {
            Iterator it = weekcovred.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry obj = (Map.Entry) it.next();
                Week week = (Week) obj.getKey();
                System.out.println("day: "+start.toString());
                System.out.println(week.getStart()+"-"+week.getEnd());
                System.out.println(week.isDayIntheWeek(start));
                //check and add to the list
                if (week.isDayIntheWeek(start)) {
                    cover = true;
                }

            }
            if (!cover) {
                if (days.get(start) != null) {
                    result.put(start, days.get(start));
                }
            } else {
                cover = false;
            }
            c.setTime(start);
            c.add(Calendar.DATE, 1);  // number of days to add
            start = c.getTime();
        }
        return result;
    }

    /**
     * This method return HashMap<Date,Path> to all dates between the start and
     * the end date.
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public Map<Week, String> getHashtagWeekIndex(String startDate, String endDate)
            throws ParseException {
        Map<Week, String> result = new HashMap<Week, String>();
        Date start = dateFormat.parse(startDate);
        Date end = dateFormat.parse(endDate);
        for (int i = 0; i < weekDatesHashtag.size(); i++) {
            if (insideWeekBoundry(start, end, weekDatesHashtag.get(i))) {
                result.put(weekDatesHashtag.get(i), weekPathsHashtag.get(i));
            }
        }
        return result;
    }

    /**
     * Return the dayLookupTweet table
     *
     * @return
     */
    public Map<String, String> getTweetLookup() {
        return dayLookupTweet;
    }

    /**
     * Return lookupHashtags
     *
     * @return
     */
    public Map<String, String> getHashtagLookup() {
        return dayLookupHashtag;
    }

    public String getTweetsPath(String key) {
        return dayLookupTweet.get(key);
    }

    public String getHashtagsPath(String key) {
        return dayLookupHashtag.get(key);
    }

    public boolean isTweetsExist(String key) {
        return dayLookupTweet.containsKey(key);
    }

    public boolean isHashtagsExist(String key) {
        return dayLookupHashtag.containsKey(key);
    }

    /**
     * This method return true if lookupdate within start,end time window
     *
     * @param start
     * @param end
     * @param lookupDate
     * @return
     */
    public static boolean insideDaysBoundry(Date start, Date end, Date lookupDate) {
        if ((lookupDate.compareTo(start) >= 0)
                && (lookupDate.compareTo(end) <= 0)) {
            return true;
        }
        return false;
    }

    public static boolean insideWeekBoundry(Date start, Date end, Week range) {
        if (insideDaysBoundry(start, end, range.getStart())
                && insideDaysBoundry(start, end, range.getEnd())) {
            return true;
        }
        return false;
    }
    
    /***
     * This method check if day exist in the missing day list or not
     * @param day
     * @return true if the day has complete dataset 
     * and False if the day miss some data
     * @throws ParseException 
     */
    public boolean isDayFromMissingDay(String day) throws ParseException{
    	Date temp = new Date(day);
    	return missingDays.contains(temp);
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        Lookup l = new Lookup();
        String path = "/home/turtle/UQUGIS/taghreed/Tools/twittercrawlermavenproject/output/result/";
        l.loadLookupTableToArrayList(path);
        String start = "2013-10-01";
        String end = "2013-12-01";
        Map<String, String> result = l.getTweetsMonthsIndex(start, end);
        System.out.println("Selected Months");
        Iterator it = result.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry obj = (Map.Entry) it.next();
            System.out.println(obj.getValue());
        }

        System.out.println("********* Missing Head non month Range *************");
        for (String t : l.getHeadofSubMonth(start, end)) {
            System.out.println(t);
        }
        
        System.out.println("********* Missing  tail non month range *************");
        for (String t : l.getTailofSubMonth(start, end)) {
            System.out.println(t);
        }
//        Map<Date,String> days = l.getTweetMissingDaysinWeek(start, end);
//        Iterator itdyas = days.entrySet().iterator();
//        while(itdyas.hasNext()){
//            Map.Entry obj = (Map.Entry)itdyas.next();
//            Date temp = (Date) obj.getKey();
//            System.out.println(temp+"\n"+obj.getValue());
//        }

    }
}
