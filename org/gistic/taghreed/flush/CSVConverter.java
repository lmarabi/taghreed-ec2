package org.gistic.taghreed.flush;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

import java.io.*;

/**
 * Created by saifalharthi on 6/9/14.
 */
public class CSVConverter {

    public void indexToCSV(Directory ramDirectory , String filePath) throws IOException {

        File file = new File(filePath);

        if(!file.exists())
        {
            file.createNewFile();
        }

        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file.getAbsolutePath() , true)));
        IndexReader reader = DirectoryReader.open(ramDirectory);

        for(int i = 0 ; i<reader.maxDoc() ; i++)
        {
            Document doc = reader.document(i);
            String created_at = doc.get("created_at");
            String tweetID = doc.get("tweet_id");
            String userID = doc.get("user_id");
            String screenName = doc.get("screen_name");
            String tweetText = doc.get("tweet_text");
            String followersCount = doc.get("followers_count");
            String lat = doc.get("lat").toString();
            String lon  = doc.get("lng").toString();

            writer.println(created_at + "," + tweetID + "," + userID + "," + screenName + "," + tweetText + "," + followersCount + "," + lat + "," + lon);


        }
        writer.close();

    }
}
