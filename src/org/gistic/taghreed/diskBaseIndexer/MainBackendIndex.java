/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseIndexer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.gistic.taghreed.Commons;

/**
 *
 * @author turtle
 */
public class MainBackendIndex {

    private String tweetsFile, hashtagsFile;
    private Commons config;
    private BuildIndex indexer;
    private BuildPyramidIndex pyramidIndexer;

//    public MainBackendIndex(String hadoopDir, String hashtagsDir, 
//            String tweetsDir, String rtreeIndexDir, String invertedIndexDir, 
//            String tweetsFile, String hashtagsFile) {
//        this.hadoopDir = hadoopDir;
//        this.hashtagsDir = hashtagsDir;
//        this.tweetsDir = tweetsDir;
//        this.rtreeIndexDir = rtreeIndexDir;
//        this.tweetsFile = tweetsFile;
//        this.hashtagsFile = hashtagsFile;
//        this.invertedIndexDir = invertedIndexDir;
//    }

    public MainBackendIndex(String tweetsFile, String hashtagsFile) throws IOException {
    	this.config = new Commons();
        this.tweetsFile = tweetsFile;
        this.hashtagsFile = hashtagsFile;
        this.indexer = new BuildIndex(tweetsFile, hashtagsFile);
        this.pyramidIndexer = new BuildPyramidIndex();
    }


    public void run(String args[]) throws FileNotFoundException, IOException {
        try {
            //Create Index in spatial hadoop
            System.out.println("Build the Day rtree index of tweets");
            indexer.CreateRtreeTweetIndex();
            System.out.println("Build the Day rtree index of hashtags");
            indexer.CreateRtreeHashtagIndex();
            System.out.println("Build the Day inverted index of tweets");
            indexer.createInvertedTweetIndex();
            System.out.println("Build the Day inverted index of hashtags");
            indexer.createInvertedHashtagIndex();
            //update lookupTable 
            System.out.println("Update the lookup table");
            indexer.UpdatelookupTable(BuildIndex.Level.Day);
            pyramidIndexer.CreateIndex();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(MainBackendIndex.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(MainBackendIndex.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(MainBackendIndex.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(MainBackendIndex.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException{
    	System.out.println(System.getProperty("user.dir"));
        File logger = new File(System.getProperty("user.dir")+"/summary.txt");
        OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(logger));
        Commons config = new Commons();
        File tweetsFile = new File(config.getTweetFlushDir());
        List<String> sortedtweetsFile;
        sortedtweetsFile = new ArrayList<String>();
        for(String file : tweetsFile.list()){
            if(!file.equals(".DS_Store")){
                sortedtweetsFile.add(config.getTweetFlushDir()+file);
            }
        }
        Collections.sort(sortedtweetsFile);
        File hashtafolder = new File(config.getHashtagFlushDir());
        List<String> sortedhashtagFile = new ArrayList<String>();
        for(String file : hashtafolder.list()){
            if(!file.equals(".DS_Store")){
                sortedhashtagFile.add(config.getHashtagFlushDir()+file);
            }
        }
        Collections.sort(sortedhashtagFile);
        if(sortedhashtagFile.size() != sortedtweetsFile.size()){
            System.out.println("Erorr hastags and tweet file doesn't match");
            return;
        }
//        BuildIndex indexer = new BuildIndex(sortedtweetsFile.get(0), sortedhashtagFile.get(0));
//        indexer.UpdatelookupTable(BuildIndex.Level.Day);
//        indexer.UpdatelookupTable(BuildIndex.Level.Week);
//        indexer.UpdatelookupTable(BuildIndex.Level.Month);
//        indexer.CreateRtreeTweetIndex();
//        indexer.CreateRtreeHashtagIndex();
//        indexer.createInvertedHashtagIndex();
//        indexer.createInvertedTweetIndex();
//        for(int i=1; i< sortedhashtagFile.size();i++){
//            BuildIndex index = new BuildIndex(sortedtweetsFile.get(i), sortedhashtagFile.get(i));
//            try {
//                index.CreateRtreeTweetIndex();
//                index.CreateRtreeHashtagIndex();
//                index.createInvertedHashtagIndex();
//                index.createInvertedTweetIndex();
//            } catch (InterruptedException ex) {
//                Logger.getLogger(MainBackendIndex.class.getName()).log(Level.SEVERE, null, ex);
//                out.write("Error in Building "+sortedtweetsFile.get(i));
//            }
//        }
        
        MainBackendIndex index = new MainBackendIndex(sortedtweetsFile.get(0), sortedhashtagFile.get(0));
        index.run(args);
        out.close();
        
    }

}
