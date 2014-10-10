/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.server;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.gistic.taghreed.collections.ActiveUsers;
import org.gistic.taghreed.collections.PopularHashtags;
import org.gistic.taghreed.collections.PopularUsers;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.collections.TweetVolumes;
import org.gistic.taghreed.diskBaseIndexer.BuildIndex;

import com.google.gson.stream.JsonWriter;

/**
 *
 * @author turtle
 */
public class HomeServer extends AbstractHandler {

    @Override
    public void handle(String s, Request baseRequest,
            HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException,
            FileNotFoundException, UnsupportedEncodingException {
        try {

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/html;charset=utf-8");
            response.addHeader("Access-Control-Allow-Origin", "*");
            response.addHeader("Content-Encoding", "gzip");
            response.addHeader("Access-Control-Allow-Credentials", "true");
            baseRequest.setHandled(true);
            String path = request.getPathInfo();

            if (path.equals("/allqueries")) {

                String query = baseRequest.getParameter("q");
                String minLong = baseRequest.getParameter("min_long");
                String minLat = baseRequest.getParameter("min_lat");
                String maxLong = baseRequest.getParameter("max_long");
                String maxLat = baseRequest.getParameter("max_lat");
                String startDate = baseRequest.getParameter("startDate");
                String endDate = baseRequest.getParameter("endDate");
                String topK = baseRequest.getParameter("topk");
                String index = (baseRequest.getParameter("index") == null) ? "Day": baseRequest.getParameter("index");
                String queryParameters = "\nMBR: (" + minLong + "-" + minLat + ") - ("
                        + maxLong + "-" + maxLat + "),StartDate: " + startDate + ",EndDate: "
                        + endDate + ",topK: " + topK + ",Keyword: " + query + ",";
                ServerRequest req = new ServerRequest();
                req.setMBR(maxLat, maxLong, minLat, minLong);
                req.setQuery(query);
                req.setStartDate(startDate);
                req.setEndDate(endDate);
                List<Tweet> tweetsResult;
                double startTime = System.currentTimeMillis();
                if (index.equals("Day")) {
                    //Tweets of querying days only
                    tweetsResult = req.getTweetsRtreeDays();
                } else {
                    //Query pyramid
                    tweetsResult = req.getTweetsRtreePyramid();
                }



                double endTime = System.currentTimeMillis();
                System.out.println("*********\n"
                        + "Found Tweets: " + tweetsResult.size() + "\n"
                        + "*************");
                System.out.println("query time = " + (endTime - startTime) + " ms");
                reportWriter.write(queryParameters + "Query-Tweets,Time (milSec)= " + (endTime - startTime) + " ms,Time (sec): " + ((endTime - startTime) / 1000) + " sec, Number of tweets= " + tweetsResult.size());

//                startTime = System.currentTimeMillis();
//                List<Tweet> tweetsResult = null;
//                if (query == null) {
//                    tweetsResult = tweetCollection;
//                } else {
//                    tweetsResult = req.getKeywordSearchFast(tweetCollection, query);
//                }
//                endTime = System.currentTimeMillis();
//                System.out.println("query time = " + (endTime - startTime) + " ms");
//                reportWriter.write(queryParameters+"Query-Tweets-KeywordSearch,Time= "+(endTime - startTime) + " ms, Number of tweets= "+tweetCollection.size());

                startTime = System.currentTimeMillis();
                List<PopularHashtags> popularHashtags;
                if(index.equals("Day")){
                    //Query Days only
                    popularHashtags = req.getHashtagsRtreeDays();
                }else{
                    //Query pyramid
                    popularHashtags = req.getHashtagsRtreePyramid();
                }
                
                

                endTime = System.currentTimeMillis();
                System.out.println("*********\n"
                        + "Found Hashtags: " + popularHashtags.size() + "\n"
                        + "*************");
                reportWriter.write(queryParameters + "Query-Hashtags,Time= " + (endTime - startTime) + " ms,Time (sec): " + ((endTime - startTime) / 1000) + " sec, Number of hashtags= " + popularHashtags.size());



                System.out.println("===========================\nTweet Result= "
                        + tweetsResult.size() + "\n");
                List<ActiveUsers> activePeople = null;
                startTime = System.currentTimeMillis();
                activePeople = req.getActivePeopleFast(tweetsResult);
                endTime = System.currentTimeMillis();
                reportWriter.write(queryParameters + "Query-ActivePeople,Time= " + (endTime - startTime) + " ms,Time (sec): " + ((endTime - startTime) / 1000) + " sec, Number of ActivePeople= " + activePeople.size());

                startTime = System.currentTimeMillis();
                List<PopularUsers> popularPeople = null;
                popularPeople = req.getPopularPeopleFast(tweetsResult);
                endTime = System.currentTimeMillis();
                reportWriter.write(queryParameters + "Query-popularPeople,Time= " + (endTime - startTime) + " ms,Time (sec): " + ((endTime - startTime) / 1000) + " sec, Number of popularPeople= " + popularPeople.size());


//                List<TweetVolumes> tweetVolumes = null;
//                tweetVolumes = req.getTweetVolumesFast(tweetsResult);

                //Retrive the topK tweets 
                startTime = System.currentTimeMillis();
                int topkInt = Integer.parseInt(topK);
                tweetsResult = req.getTopKtweets(topkInt, tweetsResult);
                endTime = System.currentTimeMillis();
                reportWriter.write(queryParameters + "Query-topkInt,Time= " + (endTime - startTime) + " \" ms,Time (sec): \"+((endTime - startTime)/1000)+\" sec, Number of topkInt= " + tweetsResult.size());
                reportWriter.write("\n-,-,-,-,-,-,-,-,-");
                reportWriter.flush();





                JsonWriter writer = new JsonWriter(new OutputStreamWriter(new GZIPOutputStream(response.getOutputStream()), "UTF-8"));
                writer.setLenient(true);
                writer.beginObject();
                writer.name("tweets");
                writer.beginArray();
                for (Tweet x : tweetsResult) {
                    writer.beginObject();
                    writer.name("created_at").value(x.created_at);
                    writer.name("tweet_id").value(x.tweetID);
                    writer.name("user_id").value(x.userID);
                    writer.name("screen_name").value(x.screenName);
                    writer.name("text").value(x.tweetText);
                    writer.name("lang").value(x.language);
                    writer.name("os").value(x.osystem);
                    writer.name("lat").value(x.lat);
                    writer.name("long").value(x.lon);
                    writer.endObject();
                }
                writer.endArray();
                //writer.endObject();
                //writer.beginObject();
                writer.name("active_people");
                writer.beginArray();
                if (activePeople.size() > 30) {
                    for (int i = 0; i < 30; i++) {
                        writer.beginObject();
                        writer.name("screen_name").value(activePeople.get(i).userName);
                        writer.name("tweet_count").value(activePeople.get(i).tweetCount);
                        writer.name("display_image").value("img/image.jpg");
                        writer.endObject();
                    }
                } else {
                    for (int i = 0; i < activePeople.size(); i++) {
                        writer.beginObject();
                        writer.name("screen_name").value(activePeople.get(i).userName);
                        writer.name("tweet_count").value(activePeople.get(i).tweetCount);
                        writer.name("display_image").value("img/image.jpg");
                        writer.endObject();
                    }
                }

                writer.endArray();
                //writer.endObject();
                //writer.beginObject();
                writer.name("popular_people");
                writer.beginArray();
                if (popularPeople.size() > 25) {
                    for (int i = 0; i < 25; i++) {
                        writer.beginObject();
                        writer.name("screen_name").value(popularPeople.get(i).screenName);
                        writer.name("followers_count").value(popularPeople.get(i).followersCount);
                        writer.name("display_image").value("img/image.jpg");
                        writer.endObject();
                    }
                } else {
                    for (int i = 0; i < popularPeople.size(); i++) {
                        writer.beginObject();
                        writer.name("screen_name").value(popularPeople.get(i).screenName);
                        writer.name("followers_count").value(popularPeople.get(i).followersCount);
                        writer.name("display_image").value("img/image.jpg");
                        writer.endObject();
                    }

                }
                writer.endArray();
                //writer.endObject();
                //writer.beginObject();
                writer.name("popular_hashtags");
                writer.beginArray();
                if (popularHashtags.size() > 50) {
                    for (int i = 0; i < 50; i++) {
                        writer.beginObject();
                        writer.name("name").value(popularHashtags.get(i).hashtagName);
                        writer.name("count").value(popularHashtags.get(i).hashtagCount);
                        writer.endObject();
                    }
                } else {
                    for (int i = 0; i < popularHashtags.size(); i++) {
                        writer.beginObject();
                        writer.name("name").value(popularHashtags.get(i).hashtagName);
                        writer.name("count").value(popularHashtags.get(i).hashtagCount);
                        writer.endObject();
                    }
                }
                writer.endArray();
                //writer.endObject();
                //writer.beginObject();
                writer.name("day_volume");
                writer.beginArray();
                Collections.sort(req.dayVolumes);
                for (TweetVolumes vol : req.dayVolumes) {
                    writer.beginObject();
                    writer.name("day").value(vol.dayName);
                    writer.name("tweet_count").value(vol.volume);
                    writer.endObject();
                }
                writer.endArray();
                writer.endObject();
                writer.close();



            } else {
                response.getWriter().print("<h2> Welcome to this tutorial </h2> <br /><br />"
                        + "These are the main functionalties that are implemeneted in this server <br /><br />"
                        + "1 - Key word Search : (input : MBR + query) <br /><br />"
                        + "<a href='http://10.14.22.10:8085/keywordSearch?q=dubai&max_lat=180&max_long=180&min_lat=-180&min_long=-180'> http://10.14.22.10:8085/keywordSearch?q=dubai&max_lat=180&max_long=180&min_lat=-180&min_long=-180 </a> <br /><br />"
                        + "2- Temporal Search (input : Start Date + End Date + MBR) <br / ><br />"
                        + "<a href='http://10.14.22.10:8085/temporalSearch?startDate=2013-10-12&endDate=2013-10-15&max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/temporalSearch?startDate=2013-10-12&endDate=2013-10-15&max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
                        + "3- Spatial Search (input : MBR) <br / ><br />"
                        + "<a href='http://10.14.22.10:8085/spatialSearch?max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/spatialSearch?max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
                        + "4- Tweets per day (input : Start Date + End Date + MBR) <br / ><br />"
                        + "<a href='http://10.14.22.10:8085/dayVolume?startDate=2013-10-12&endDate=2013-10-15&max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/dayVolume?startDate=2013-10-12&endDate=2013-10-15&max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
                        + "5- Hashtag Count (input : MBR) <br / ><br />"
                        + "<a href='http://10.14.22.10:8085/hashtagcount?max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/hashtagcount?max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
                        + "6- Popular People (input : MBR) <br / ><br />"
                        + "<a href='http://10.14.22.10:8085/popularPerson?max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/popularPerson?max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
                        + "7- Active users (input : MBR) <br / ><br />"
                        + "<a href='http://10.14.22.10:8085/activePerson?max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/activePerson?max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
                        + "<h1> Thank You </h1>");
            }

        } catch (ParseException e) {
            System.out.println("error happened");
        }




    }
    private static String file = System.getProperty("user.dir") + "/report.csv";
    private static OutputStreamWriter reportWriter = null;

    public static void main(String[] args) throws Exception {
        BuildIndex indexer = new BuildIndex();
        indexer.UpdatelookupTable(BuildIndex.Level.Week);
        indexer.UpdatelookupTable(BuildIndex.Level.Day);
        indexer.UpdatelookupTable(BuildIndex.Level.Month);
        Server server = new Server(8085);
        server.setHandler(new HomeServer());
        reportWriter = new OutputStreamWriter(new FileOutputStream(
                file, true), "UTF-8");
        server.start();
        server.join();
    }
}
