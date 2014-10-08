package org.gistic.taghreed.dataanalysis.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.gistic.taghreed.dataanalysis.analyzer.ArabicNormalizer;

/**
 * Search the index
 *
 * @author Amgad Madkour
 */
public class Searcher {

    IndexSearcher indexSearcher;

    public Searcher() {
    }

    public void createIndexSearcher(Directory dir) {

        IndexReader indexReader;
        IndexSearcher indxS;

        try {
            indexReader = DirectoryReader.open(dir);
            indxS = new IndexSearcher(indexReader);
            this.indexSearcher = indxS;
        } catch (IOException ex) {
            Logger.getLogger(Searcher.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public ArrayList<Document> searchTweets(String value) {

        ArabicNormalizer normalizer;
        PhraseQuery query;
        int numResults = 100;
        ArrayList<Document> docs;
        Analyzer analyzer;
        String field = "normalized";
        String[] terms;

        docs = new ArrayList<Document>();
        
        try {
            
            analyzer = new WhitespaceAnalyzer(Version.LUCENE_47);
            normalizer = new ArabicNormalizer();

            query = new PhraseQuery();
            terms = normalizer.normalize(value).split(" ");
            
            for(String term: terms){
                query.add(new Term(field, term));
            }
            
            ScoreDoc[] hits = indexSearcher.search(query, numResults).scoreDocs;

            for (ScoreDoc hit : hits) {
                Document doc = indexSearcher.doc(hit.doc);
                docs.add(doc);
            }
            
            analyzer.close();

        } catch (IOException ex) {
            Logger.getLogger(Searcher.class.getName()).log(Level.SEVERE, null, ex);
        }

        return docs;
    }

}
