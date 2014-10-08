package org.gistic.taghreed.flush;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.gistic.taghreed.Commons;
import org.gistic.taghreed.crawler.Crawler;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by saifalharthi on 6/12/14.
 */
public class TimedFlush extends TimerTask {


    @Override
    public void run() {
        Commons commons = null;
        try {
            commons = new Commons();
        } catch (IOException ex) {
            Logger.getLogger(TimedFlush.class.getName()).log(Level.SEVERE, null, ex);
        }

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE , -1);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String formattedDate = sdf.format(cal.getTime());

        File file = new File(commons.getTweetFlushDir() + "/"+ formattedDate + ".csv");

        if(!file.exists())
        {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        PrintWriter writer = null;
        try {
            writer = new PrintWriter(new BufferedWriter(new FileWriter(file.getAbsolutePath() , true)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(Crawler.ramDir);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(int i = 0 ; i<reader.maxDoc() ; i++) {
            Document doc = null;
            try {
                doc = reader.document(i);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String created_at = doc.get("created_at");
            String tweetID = doc.get("tweet_id");
            String userID = doc.get("user_id");
            String screenName = doc.get("screen_name");
            String tweetText = doc.get("tweet_text");
            String followersCount = doc.get("followers_count");
            String lat = doc.get("lat").toString();
            String lon = doc.get("lng").toString();

            if (created_at.contains(formattedDate)) {

                writer.println(created_at + "," + tweetID + "," + userID + "," + screenName + "," + tweetText + "," + followersCount + "," + lat + "," + lon);
            }

        }
        writer.close();

        // Execute Index Builder
    }
}
