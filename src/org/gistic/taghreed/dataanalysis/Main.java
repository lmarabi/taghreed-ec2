package org.gistic.taghreed.dataanalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.gistic.taghreed.dataanalysis.datasource.DataSourceWrapper;
import org.gistic.taghreed.dataanalysis.search.Indexer;
import org.gistic.taghreed.dataanalysis.search.Searcher;
import org.gistic.taghreed.dataanalysis.twitter.Tweet;

/**
 * Taghreed Data Analyzer , Test File
 *
 * @author Amgad Madkour
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        Directory dir;
        Searcher searcher;
        ArrayList<Document> docs;
        String query="";

        searcher = new Searcher();

        System.out.println("Building Index");
        dir = buildIndex();
        searcher.createIndexSearcher(dir);
        System.out.println("Index Created");

        do {
            try {
                BufferedReader rdr = new BufferedReader(new InputStreamReader(System.in));
                System.out.print("Query : ");
                query = rdr.readLine();
                docs = searcher.searchTweets(query); //Search tweet method
                
                System.out.println("=== Results ===");
                for(Document doc:docs){
                    System.out.println(doc.get("text"));
                }
                System.out.println("===============");
                
            } catch (IOException ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        } while (!query.equals("quit"));
        
        System.out.println("Done.");

    }

    public static Directory buildIndex() {

        DataSourceWrapper wrapper;
        ArrayList<Tweet> tweets;
        Indexer indx;

        wrapper = new DataSourceWrapper();
        indx = new Indexer();

        tweets = wrapper.getFromMongo();

        try {

            for (Tweet tweet : tweets) {
                indx.indexTweet(tweet); //Index the Tweet method
            }

            System.out.println("Indexed : " + indx.getIndexSize());
            indx.closeIndexWriter();

        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

        return indx.getDirectory();
    }
}
