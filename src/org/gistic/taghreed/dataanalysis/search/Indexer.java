package org.gistic.taghreed.dataanalysis.search;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.gistic.taghreed.dataanalysis.analyzer.ArabicNormalizer;
import org.gistic.taghreed.dataanalysis.twitter.Tweet;

/**
 * Create a Lucene Index
 * 
 * @author Amgad Madkour
 */
public class Indexer {

    private RAMDirectory ram;
    private ArabicNormalizer normalizer;
    private IndexWriter indexWriter;
    
    /**
     * Creates a new instance of Indexer
     */
    public Indexer() {
        //Prepare the Arabic normalizer
        normalizer = new ArabicNormalizer();
        indexWriter = null;
    }

    private IndexWriter getIndexWriter(boolean create) throws IOException {

        if (indexWriter == null || create == true) {
            ram = new RAMDirectory();
            Analyzer stdAn = new WhitespaceAnalyzer(Version.LUCENE_45);
            IndexWriterConfig iwConf = new IndexWriterConfig(Version.LUCENE_45, stdAn);
            indexWriter= new IndexWriter(ram, iwConf);
        }
        return indexWriter;
    }

    public void closeIndexWriter() throws IOException {
        if (indexWriter != null) {
            indexWriter.commit();
            indexWriter.close();
        }
    }

    public void indexTweet(Tweet tweet) throws IOException {

        String normalized;
        Document doc;
        IndexWriter writer;
        
        writer = getIndexWriter(false);
        doc = new Document();

        //Create normalized text
        normalized = normalizer.normalize(tweet.getText());
        doc.add(new TextField("normalized", normalized, Field.Store.NO));
        doc.add(new StringField("tweet_id", String.valueOf(tweet.getTweetId()), Field.Store.YES));
        doc.add(new StringField("screen_name", tweet.getScreenName(), Field.Store.YES));
        doc.add(new StringField("created_at", tweet.getCreatedAt(), Field.Store.YES));
        doc.add(new StringField("followers_count", String.valueOf(tweet.getFollowersCount()), Field.Store.YES));
        doc.add(new StringField("lang", tweet.getLang(), Field.Store.YES));
        doc.add(new StringField("lat", String.valueOf(tweet.getLatitude()), Field.Store.YES));
        doc.add(new StringField("long", String.valueOf(tweet.getLongitude()), Field.Store.YES));
        doc.add(new StringField("user_id", String.valueOf(tweet.getUserId()), Field.Store.YES));
        doc.add(new StringField("text", tweet.getText(), Field.Store.YES));
        doc.add(new StringField("source" , tweet.getSource() , Field.Store.YES));
        
        writer.addDocument(doc);
    }

    public void rebuildIndexes(ArrayList<Tweet> tweets) throws IOException {

        getIndexWriter(true);
        
        for (Tweet tweet : tweets) {
            indexTweet(tweet);
        }

        closeIndexWriter();
    }
    
    public Directory getDirectory(){
        return ram;
    }
    
    public int getIndexSize(){
        return indexWriter.numDocs();
    }
}
