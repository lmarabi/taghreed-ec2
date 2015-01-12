/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.query;

import org.gistic.taghreed.collections.Partition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gistic.invertedIndex.KWIndexSearcher;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.Hashtag;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.collections.TweetVolumes;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQueryOptimizer.Grid;
import org.gistic.taghreed.diskBaseQueryOptimizer.GridCell;

/**
 *
 * @author turtle
 */
public class PyramidQueryProcessor {

    private OutputStreamWriter outwriter;
    private MBR area;
    private static double startTime;
    private static double endTime;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private int topK;
    private String query;
    private List<TweetVolumes> dayVolume = new ArrayList<TweetVolumes>();
    private Map<Date, Integer> weekVolume = new HashMap<Date, Integer>();
    private static ServerRequest serverRequest;

    public static enum QueryRequest {

        tweet, hashtag
    };
    public static QueryRequest queryType;

    public PyramidQueryProcessor(ServerRequest serverRequest) {
        this.serverRequest = serverRequest;
    }

    public void setWeekVolume(Map<Date, Integer> weekVolume) {
        this.weekVolume = weekVolume;
    }

    /**
     * This metho will read from the nodes inside the R-tree index
     *
     *
     * @param dataPath
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     * @throws IOException
     * @throws CompressorException
     */
    public void GetSmartOutput(String day,String dataPath)
            throws FileNotFoundException,
            UnsupportedEncodingException, IOException, ParseException {
        dataPath += "/";
        if (serverRequest.getIndex().equals(ServerRequest.queryIndex.rtree)) {
            // Get the set of Files that intersect with the area.
            List<Partition> files = ReadMaster(day,dataPath);
            logEnd("selected (" + files.size() + ")");
            //read eachfile and output the result.
            for (Partition f : files) {
                System.out.println("Start Reading file " + f.getPartition().getName());
                int partitionCount = smartQuery(f, outwriter);
                System.out.println("Select "+partitionCount+" out of "
                		+f.getCardinality()+" Selectivity is: "
                		+(double)(((double)partitionCount/(double)f.getCardinality())*100)+" %");
            }
        }else{
            GetDocumentsInvertedIndex(dataPath);
        }

        logEnd("end reading files");

    }
    
  

    /**
     * This method get documents based tweets/hashtags based on the
     * ServerRequest
     *
     * @param indexPath
     * @return
     * @throws IOException
     * @throws ParseException
     */
    public int GetDocumentsInvertedIndex(String indexPath) throws IOException, ParseException {
        int count = 0;
        List<String> documents;
        //Read the documents from inverted index 
        KWIndexSearcher indexSearcher = new KWIndexSearcher();
        if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
            documents = indexSearcher.search(indexPath,
                    KWIndexSearcher.dataType.tweets,
                    serverRequest.getQuery(), 5000);
            //SpatialFilter to the documents
            for (String doc : documents) {
                Tweet tweetObj = new Tweet(doc);
                if (serverRequest.getMbr().insideMBR(new Point(tweetObj.lat,
                        tweetObj.lon))) {
                    Date date = Tweet.parseTweetTimeToDate(tweetObj.created_at);
                    if (weekVolume.containsKey(date)) {
                        weekVolume.put(date, weekVolume.get(date) + 1);
                    } else {

                        weekVolume.put(date, 1);
                    }
                    outwriter.write(doc);
                    outwriter.write("\n");
                    count++;
                }
            }
        } else {
            documents = indexSearcher.search(indexPath,
                    KWIndexSearcher.dataType.hashtags,
                    serverRequest.getQuery(), 5000);
            //SpatialFilter to the documents
            for (String doc : documents) {
                Hashtag hashtag = new Hashtag(doc);
                if (serverRequest.getMbr().insideMBR(new Point(hashtag.getLat(),
                        hashtag.getLon()))) {
                    outwriter.write(doc);
                    outwriter.write("\n");
                    count++;
                }
            }
        }

        return count;
    }

    /**
     * This method Write the Final result .
     *
     * @param f
     * @param out Streamer
     * @throws FileNotFoundException
     * @throws IOException
     */
    public int smartQuery(Partition part, OutputStreamWriter output)
            throws FileNotFoundException, IOException, ParseException {
        BufferedReader reader;
//    FileInputStream fin = new FileInputStream(part.getPartition());
//    BufferedInputStream bis = new BufferedInputStream(fin);
//    CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
//    BufferedReader reader = new BufferedReader(new InputStreamReader(input,"UTF-8"));
        reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(part.getPartition()), "UTF-8"));
        String line;
        int count = 0;
        //get the range file
        while ((line = reader.readLine()) != null) {
            //Here rather than wrting to local storage you can pass it 
            //right away to Visualization team.
            Point node;
            if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
                Tweet tweetObj = new Tweet(line);
                node = new Point(tweetObj.lat, tweetObj.lon);
                if (serverRequest.getMbr().insideMBR(node)) {
                    if (serverRequest.getQuery() != null) {
                        if (tweetObj.tweet_text.contains(serverRequest.getQuery())) {
                            Date date = Tweet.parseTweetTimeToDate(tweetObj.created_at);
                            if (weekVolume.containsKey(date)) {
                                weekVolume.put(date, weekVolume.get(date) + 1);
                            } else {

                                weekVolume.put(date, 1);
                            }
                            count++;
                            output.write(line);
                            output.write("\n");
                        }
                    } else {
                        Date date = Tweet.parseTweetTimeToDate(tweetObj.created_at);
                        if (weekVolume.containsKey(date)) {
                            weekVolume.put(date, weekVolume.get(date) + 1);
                        } else {
                            weekVolume.put(date, 1);
                        }
                        count++;
                        output.write(line);
                        output.write("\n");
                    }

                }
            } else {
                Hashtag hashtag = new Hashtag(line);
                node = new Point(hashtag.getLat(), hashtag.getLon());
                if (serverRequest.getMbr().insideMBR(node)) {
                    if (serverRequest.getQuery() != null) {
                        if (hashtag.getHashtagText().contains(serverRequest.getQuery())) {
                            output.write(line);
                            output.write("\n");
                        }
                    } else {
                        output.write(line);
                        output.write("\n");
                    }

                }
            }

        }
        reader.close();
        return count;
    }

    /**
     * output message and start a timer to check the time
     *
     * @param text
     */
    public static void logStart(String text) {
        startTime = System.currentTimeMillis();
        System.out.println(text);
    }

    /**
     * End the message and show the time in second
     *
     * @param text
     */
    public static void logEnd(String text) {
        endTime = System.currentTimeMillis();
        System.out.println(text + " which took"
                + " " + ((endTime - startTime) / 1000) + " seconds");
    }

    /**
     * This Method read all the files in the data directory and fetch only the
     * Intersect files
     *
     * @param maxLat
     * @param minLat
     * @param maxLon
     * @param minLon
     * @param path
     * @return
     */
    private List<Partition> ReadMaster(String day,String path)
            throws FileNotFoundException, IOException {
        File master;
        List<Partition> result = new ArrayList<Partition>();
        master = new File(path + "/_master.quadtree");
		if(!master.exists()){
			master = new File(path + "/_master.str");
			if(!master.exists()){
				master = new File(path + "/_master.str+");
				if(!master.exists()){
					master = new File(path + "/_master.grid");
				}
			}
		}
        BufferedReader reader = new BufferedReader(new FileReader(master));
//        FileInputStream fin = new FileInputStream(master);
//        BufferedInputStream bis = new BufferedInputStream(fin);
//        CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
//        BufferedReader reader = new BufferedReader(new InputStreamReader(input, "UTF-8"));
         String line = null;
        while ((line = reader.readLine()) != null) {
            String[] temp = line.split(",");
            // The file has the following format as Aggreed with the interface
            // between hadoop and this program 
            // #filenumber,minLat,minLon,maxLat,maxLon
            //0,minLon,MinLat,MaxLon,MaxLat,Filename
            if (temp.length == 8) {
                Partition part = new Partition(line,path,day);
                if (serverRequest.getMbr().Intersect(
                        part.getArea().getMax(), part.getArea().getMain())) {
                    result.add(part);
                }
            }
        }
        reader.close();
        return result;
    }

    /**
     * Create output Stream for result
     *
     * @param exportPath
     * @param id
     * @throws UnsupportedEncodingException
     * @throws FileNotFoundException
     */
    public void createStreamer(String exportPath)
            throws UnsupportedEncodingException, FileNotFoundException {
        outwriter = new OutputStreamWriter(new FileOutputStream(
                exportPath + "result.txt"), "UTF-8");
    }

    /**
     * Close output streamer
     *
     * @throws IOException
     */
    public void closeStreamer() throws IOException {
        outwriter.close();
    }

    public OutputStreamWriter getStreamer() {
        return this.outwriter;
    }

    /**
     * This method return the dayVolume
     *
     * @return
     * @throws ParseException
     */
    public List<TweetVolumes> getTweetVolume() throws ParseException {
        Iterator it = weekVolume.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry obj = (Map.Entry) it.next();
            dayVolume.add(new TweetVolumes((Date) obj.getKey(),
                    (Integer) obj.getValue()));
        }
        Collections.sort(dayVolume);
        return dayVolume;
    }

    /**
     * Add Tweet volume
     *
     * @param date
     * @param volume
     * @throws ParseException
     */
    public void addTweetVolume(Date date, int volume) throws ParseException {
        if (!weekVolume.containsKey(date)) {
            weekVolume.put(date, volume);
        } else {
            int v = weekVolume.get(date);
            volume += v;
            weekVolume.put(date, volume);
        }

    }

    /**
     * This method assign the output writer to the given parameter
     *
     * @param writer
     */
    public void parseStreamer(OutputStreamWriter writer) {
        this.outwriter = writer;
    }
}
