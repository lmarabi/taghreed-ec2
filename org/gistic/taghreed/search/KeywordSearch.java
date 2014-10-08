package org.gistic.taghreed.search;

import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.crawler.Crawler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by saifalharthi on 5/27/14.
 */
public class KeywordSearch {

    public List<Tweet> searchIndex(String maxLat, String maxLong, String minLat, String minLong, String startDate, String endDate, String keyword) throws ParseException, IOException {


        MBR mbr = new MBR(new Point(maxLat , maxLong) , new Point(minLat , minLong));
        List<Tweet> resultList = new ArrayList<Tweet>();
        if(DirectoryReader.indexExists(Crawler.ramDir)) {

            System.out.println("Found Index!!");
            IndexReader reader = null;
            try {
                reader = IndexReader.open(Crawler.ramDir);
                int count = reader.numDocs();
                System.out.println("Number of Indexed Docs: " + count);
            } catch (IOException e) {
                e.printStackTrace();
            }
            IndexSearcher searcher = new IndexSearcher(reader);

            Query query = new QueryParser(Version.LUCENE_47, "tweet_text", new ArabicAnalyzer(Version.LUCENE_47)).parse(keyword);

            TopDocs topDocs = searcher.search(query, Integer.MAX_VALUE);
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
                String created_at = doc.get("created_at");
                String tweet_id = doc.get("tweet_id");
                String user_id = doc.get("user_id");
                String screen_name = doc.get("screen_name");
                String tweet_text = doc.get("tweet_text");
                String latitude = doc.get("lat").toString();
                String longitude = doc.get("lng").toString();
                int followersCount = Integer.parseInt(doc.get("followers_count"));

                Tweet tweet = new Tweet(created_at , tweet_id , user_id , screen_name , tweet_text , latitude , longitude , followersCount);
                if(mbr.insideMBR(new Point(latitude, longitude)))
                resultList.add(tweet);


            }
        }
        else
            System.out.println("Index NOT FOUND!!!!!!!!!!!!");
        System.out.println(resultList.size());
        return resultList;
    }
}
