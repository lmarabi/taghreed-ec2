package org.gistic.taghreed.search;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.store.Directory;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.crawler.Crawler;

import javax.print.Doc;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by saifalharthi on 5/28/14.
 */
public class SpatialSearch {

    private SpatialContext ctx;
    private SpatialStrategy strategy;

    public SpatialSearch()
    {
        ctx = new SpatialContext(false , null , new RectangleImpl(-180 , 180 , -180 ,180 , ctx));
        SpatialPrefixTree tree = new QuadPrefixTree(ctx , 11);

        this.strategy = new RecursivePrefixTreeStrategy(tree ,"location");
    }

    public List<Tweet> Search(double maxLat , double minLat , double maxLon , double minLon , String startDate , String endDate) throws IOException {
        IndexReader reader = DirectoryReader.open(Crawler.quadRamDir);

        IndexSearcher searcher = new IndexSearcher(reader);
        List<Tweet> results = new ArrayList<Tweet>();
        Shape rect = new RectangleImpl(minLat,maxLat , minLon ,maxLon , ctx);
        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects , rect);
        Filter filter = strategy.makeFilter(args);

        try
        {
            System.out.println(searcher.getIndexReader().numDocs());
            TopDocs retDocs = searcher.search(new MatchAllDocsQuery() , filter , searcher.getIndexReader().numDocs());
            int Hits = retDocs.totalHits;
            for(int i = 0 ; i < Hits ; i++)
            {
                Document doc = searcher.doc(retDocs.scoreDocs[i].doc);
                String created_at = doc.get("created_at");
                String tweet_id = doc.get("tweet_id");
                String user_id = doc.get("user_id");
                String screen_name = doc.get("screen_name");
                String tweet_text = doc.get("tweet_text");
                String coordinates = doc.get("coords");
                String[] splitCoordinates = coordinates.split(" ");
                int followers_count = Integer.parseInt(doc.get("followers_count"));

                Tweet tweet = new Tweet(created_at , tweet_id , user_id , screen_name , tweet_text , splitCoordinates[0] , splitCoordinates[1] , followers_count);
                results.add(tweet);
            }
        }
        catch (Exception e)
        {

        }

        return results;
    }



}
