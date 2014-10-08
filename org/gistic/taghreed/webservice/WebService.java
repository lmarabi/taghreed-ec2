package org.gistic.taghreed.webservice;

import com.google.gson.stream.JsonWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.gistic.taghreed.Commons;
import org.gistic.taghreed.collections.ActiveUsers;
import org.gistic.taghreed.collections.PopularUsers;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.collections.TweetVolumes;
import org.gistic.taghreed.crawler.Crawler;
import org.gistic.taghreed.flush.CSVConverter;
import org.gistic.taghreed.search.KeywordSearch;
import org.gistic.taghreed.search.Results;
import org.gistic.taghreed.search.SpatialSearch;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.LocalDate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.Result;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Created by saifalharthi on 5/18/14.
 */



public class WebService extends AbstractHandler {


    @Override
    public void handle(String path, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException{

        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/html;charset=utf-8");
        response.addHeader("Access-Control-Allow-Origin", "*");
        response.addHeader("Content-Encoding","gzip");
        response.addHeader("Access-Control-Allow-Credentials", "true");
        baseRequest.setHandled(true);



        if (path.equals("/allqueries"))
        {
            long startTime = System.currentTimeMillis();
            Results res = new Results();
            List<Tweet> tweetResults = null;
            String query = baseRequest.getParameter("q");
            String minLong = baseRequest.getParameter("min_long");
            String minLat = baseRequest.getParameter("min_lat");
            String maxLong = baseRequest.getParameter("max_long");
            String maxLat = baseRequest.getParameter("max_lat");
            String startDate = baseRequest.getParameter("startDate");
            String endDate = baseRequest.getParameter("endDate");


            if(query == null)
            {
                SpatialSearch spatialSearch = new SpatialSearch();
                tweetResults = spatialSearch.Search(Double.parseDouble(maxLat) , Double.parseDouble(minLat) , Double.parseDouble(maxLong) , Double.parseDouble(minLong) , startDate , endDate);


            }
            else
            {
                KeywordSearch keywordSearch = new KeywordSearch();
                try {
                    tweetResults = keywordSearch.searchIndex(maxLat , maxLong ,minLat , minLong , startDate , endDate , query );
                    System.out.println("Entered Keyword Search");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            List<ActiveUsers> activeUsers = res.getActiveUsers(tweetResults);
            List<PopularUsers> popularUsers = res.getPopularUsers(tweetResults);
            List<TweetVolumes> tweetVolumes = res.getTweetVolumes(tweetResults);

            long endTime = System.currentTimeMillis();

            System.out.println("Time before JSON Rendering : " + (endTime - startTime));

            JsonWriter writer = new JsonWriter(new OutputStreamWriter(new GZIPOutputStream(response.getOutputStream()), "UTF-8"));
            writer.setLenient( true );
            writer.beginObject();
            writer.name("tweets");
            writer.beginArray();
            for(Tweet x: tweetResults){
                writer.beginObject();
                writer.name("created_at").value(x.created_at);
                writer.name("tweet_id").value(x.tweetID);
                writer.name("user_id").value(x.userID);
                writer.name("screen_name").value(x.screenName);
                writer.name("text").value(x.tweetText);
                writer.name("lat").value(x.lat);
                writer.name("long").value(x.lon);
                writer.endObject();
            }
            writer.endArray();
            //writer.endObject();
            //writer.beginObject();
            writer.name("active_people");
            writer.beginArray();
            if(activeUsers.size() > 30)
            {
                for(int i = 0 ; i < 30 ; i++)
                {
                    writer.beginObject();
                    writer.name("screen_name").value(activeUsers.get(i).userName);
                    writer.name("tweet_count").value(activeUsers.get(i).tweetCount);
                    writer.name("display_image").value("img/image.jpg");
                    writer.endObject();
                }
            }
            else
            {
                for(int i = 0 ; i < activeUsers.size() ; i++)
                {
                    writer.beginObject();
                    writer.name("screen_name").value(activeUsers.get(i).userName);
                    writer.name("tweet_count").value(activeUsers.get(i).tweetCount);
                    writer.name("display_image").value("img/image.jpg");
                    writer.endObject();
                }
            }

            writer.endArray();
            //writer.endObject();
            //writer.beginObject();
            writer.name("popular_people");
            writer.beginArray();
            if(popularUsers.size() > 25)
            {
                for(int i = 0 ; i < 25 ; i++)
                {
                    writer.beginObject();
                    writer.name("screen_name").value(popularUsers.get(i).screenName);
                    writer.name("followers_count").value(popularUsers.get(i).followersCount);

                    writer.endObject();
                }
            }
            else
            {
                for(int i = 0 ; i < popularUsers.size() ; i++)
                {
                    writer.beginObject();
                    writer.name("screen_name").value(popularUsers.get(i).screenName);
                    writer.name("followers_count").value(popularUsers.get(i).followersCount);

                    writer.endObject();
                }

            }
            writer.endArray();
            //writer.endObject();
            //writer.beginObject();
            writer.name("popular_hashtags");
            writer.beginArray();
            //Hashtags Later
            writer.endArray();
            //writer.endObject();
            //writer.beginObject();
            writer.name("day_volume");
            writer.beginArray();
            for(TweetVolumes vol : tweetVolumes)
            {
                writer.beginObject();
                writer.name("day").value(vol.dayName);
                writer.name("tweet_count").value(vol.volume);
                writer.endObject();
            }
            writer.endArray();
            writer.endObject();
            writer.close();



        }
        else if(path.equals("/flushnow"))
        {
            String fileName = baseRequest.getParameter("name");

            CSVConverter converter = new CSVConverter();
            converter.indexToCSV(Crawler.ramDir , "/Users/saifalharthi/Projects/ShahedDemo/Flush/" + fileName + ".csv");
            System.out.println("Done Flushing");
        }

    }

    private List<Date> getDatesBetween(String startDate , String endDate)
    {
        return null;
    }
}