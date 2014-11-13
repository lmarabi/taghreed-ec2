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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.gistic.invertedIndex.KWIndexSearcher;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.Hashtag;
import org.gistic.taghreed.collections.PopularHashtags;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.collections.TweetVolumes;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQueryOptimizer.GridCell;

/**
 *
 * @author turtle
 */
public class DayQueryProcessor {

    //Global lookup
    public static Lookup lookup = new Lookup();
    public static List<TweetVolumes> dayVolume;
    private static OutputStreamWriter outwriter;
    private static double startTime;
    private static double endTime;
    private Date startDate;
    private Date endDate;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private int topK;
    private static Map<Date, Integer> tweetsVolume = new HashMap<Date, Integer>();
    private static ServerRequest serverRequest;

    public OutputStreamWriter getStreamer() {
        return outwriter;
    }

    public DayQueryProcessor(ServerRequest request) throws IOException, FileNotFoundException, ParseException {
        this.serverRequest = request;
        this.dayVolume = new ArrayList<TweetVolumes>();
        //Load lookuptabe
        if (serverRequest.getIndex().equals(ServerRequest.queryIndex.rtree)) {
            lookup.loadLookupTableToArrayList(this.serverRequest.getRtreeDir());
        } else {
            lookup.loadLookupTableToArrayList(this.serverRequest.getInvertedDir());
        }
    }

    public DayQueryProcessor(String startDate, String endDate) throws ParseException {
        this.startDate = dateFormat.parse(startDate);
        this.endDate = dateFormat.parse(endDate);
    }

    /**
     * This metho will read from the nodes inside the R-tree index
     *
     * @param id
     * @param maxLat
     * @param maxLon
     * @param minLat
     * @param minLon
     * @param dataPath
     * @param exportPath
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     * @throws IOException
     * @throws CompressorException
     */
    public  int GetSmartOutput(String day, String dataPath)
            throws FileNotFoundException,
            UnsupportedEncodingException, IOException, ParseException {
        int count = 0;
        //Set the area of interest in the MBR 
        dataPath += "/";
        if (serverRequest.getIndex().equals(ServerRequest.queryIndex.rtree)) {
            // Get the set of Files that intersect with the area.
            
            List<Partition> files = ReadMaster(day,dataPath);
            logEnd("selected (" + files.size() + ")");
            //read eachfile and output the result.
            for (Partition f : files) {
                System.out.println("Start Reading file " + f.getPartition().getName());
                int partitionCount = smartQuery(f, outwriter); 
                count +=  partitionCount;
                System.out.println("Select "+partitionCount+" out of "
                		+f.getCardinality()+" Selectivity is: "
                		+(double)(((double)partitionCount/(double)f.getCardinality())*100)+" %");
            }
        }else{
             count += GetDocumentsInvertedIndex(dataPath);
        }
        logEnd("end reading files");
        return count;
    }

    /**
     * This metho will read from the nodes inside the R-tree index
     *
     * @param id
     * @param maxLat
     * @param maxLon
     * @param minLat
     * @param minLon
     * @param dataPath
     * @param exportPath
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     * @throws IOException
     * @throws CompressorException
     */
    public void GetSmartOutputCheckTemporal(String day,String dataPath)
            throws FileNotFoundException,
            UnsupportedEncodingException, IOException, ParseException {
        dataPath += "/";
        // Get the set of Files that intersect with the area.
        logStart("start reading the master files");
        List<Partition> files = ReadMaster(day,dataPath);
        logEnd("end reading master file and selected (" + files.size() + ")");
        //read eachfile and output the result.
        logStart("start reading from selected files");
        for (Partition f : files) {
            System.out.println("Start Reading file " + f.getPartition().getName());
            smartQueryChechTemporal(f, outwriter);
            System.out.println("End reading file " + f.getPartition().getName());
        }

        logEnd("end reading files");
    }

    /**
     * This method Write the Final result .
     *
     * @param f
     * @param out Streamer
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static int smartQuery(Partition part, OutputStreamWriter output)
            throws FileNotFoundException, IOException, ParseException {
        BufferedReader reader;
//    FileInputStream fin = new FileInputStream(part.getPartition());
//    BufferedInputStream bis = new BufferedInputStream(fin);
//    CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
//    BufferedReader reader = new BufferedReader(new InputStreamReader(input,"UTF-8"));
        reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(part.getPartition()), "UTF-8"));
        String tweet;
        int count = 0;
        //get the range file
        while ((tweet = reader.readLine()) != null) {
            //Here rather than wrting to local storage you can pass it 
            //right away to Visualization team.

            Point node;
            if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
                Tweet tweetObj = new Tweet(tweet);
                node = new Point(tweetObj.lat, tweetObj.lon);
                if (serverRequest.getMbr().insideMBR(node)) {
                    if (serverRequest.getQuery() != null) {
                        if (tweetObj.tweetText.contains(serverRequest.getQuery())) {
                            output.write(tweet);
                            output.write("\n");
                            count++;
                            insertTweetsToVolume(tweetObj.created_at);
                        }
                    } else {
                        output.write(tweet);
                        output.write("\n");
                        count++;
                        insertTweetsToVolume(tweetObj.created_at);
                    }

                }
            } else {
                Hashtag hashtag = new Hashtag(tweet);
                node = new Point(hashtag.getLat(), hashtag.getLon());
                if (serverRequest.getMbr().insideMBR(node)) {
                    if (serverRequest.getQuery() != null) {
                        if (hashtag.getHashtagText().contains(serverRequest.getQuery())) {
                            output.write(tweet);
                            output.write("\n");
                            count++;
                        }
                    } else {
                        output.write(tweet);
                        output.write("\n");
                        count++;
                    }

                }
            }

        }
        reader.close();
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
    public void smartQueryChechTemporal(Partition part, OutputStreamWriter output)
            throws FileNotFoundException, IOException, ParseException {

        BufferedReader reader;
//    FileInputStream fin = new FileInputStream(part.getPartition());
//    BufferedInputStream bis = new BufferedInputStream(fin);
//    CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
//    BufferedReader reader = new BufferedReader(new InputStreamReader(input,"UTF-8"));
        reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(part.getPartition()), "UTF-8"));
        String tweet;
        //get the range file
        while ((tweet = reader.readLine()) != null) {
            //Here rather than wrting to local storage you can pass it 
            //right away to Visualization team.
            String[] temp = tweet.split(",");
            Point node;
            if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
                node = new Point(temp[8], temp[9]);
            } else {
                node = new Point(temp[0], temp[1]);
            }
            if (serverRequest.getMbr().insideMBR(node)) {
                if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
                    String[] datetemp = temp[0].split(" ");
                    Date date = dateFormat.parse(datetemp[0]);
                    if (org.gistic.taghreed.diskBaseQuery.query.Lookup.insideDaysBoundry(this.startDate, this.endDate, date)) {
                        output.write(tweet);
                        output.write("\n");
                    }
                } else {
                    output.write(tweet);
                    output.write("\n");
                }

            }

        }
        reader.close();
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
    private static List<Partition> ReadMaster(String day,String path)
            throws FileNotFoundException, IOException {
        File master;
        List<Partition> result = new ArrayList<Partition>();
        master = new File(path + "/_master.str+");
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
    public static void createStreamer()
            throws UnsupportedEncodingException, FileNotFoundException, IOException {
        File f = new File(serverRequest.getOutputResult());
        if(!f.exists()){
            File dir = new File(f.getParent());
            if(!dir.exists()){
                dir.mkdirs();
            }
            f.createNewFile();
        }
        outwriter = new OutputStreamWriter(new FileOutputStream(
                serverRequest.getOutputResult()), "UTF-8");
    }

    /**
     * This method assign the output writer to the given parameter
     *
     * @param writer
     */
    public void parseStreamer(OutputStreamWriter writer) {
        this.outwriter = writer;
    }

    /**
     * Close output streamer
     *
     * @throws IOException
     */
    public static void closeStreamer() throws IOException {
        outwriter.close();
    }

    private static void insertTweetsToVolume(String day) throws ParseException {
        if (tweetsVolume.containsKey(Tweet.parseTweetTimeToDate(day))) {
            tweetsVolume.put(Tweet.parseTweetTimeToDate(day),
                    (tweetsVolume.get(Tweet.parseTweetTimeToDate(day)) + 1));
        } else {
            tweetsVolume.put(Tweet.parseTweetTimeToDate(day), 1);
        }
    }

    public List<TweetVolumes> getTweetsVolum() throws ParseException {
        List<TweetVolumes> result = new ArrayList<TweetVolumes>();
        Iterator it = tweetsVolume.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry obj = (Map.Entry) it.next();
            result.add(new TweetVolumes((Date) obj.getKey(),
                    (Integer) obj.getValue()));
        }
        Collections.sort(result);
        return result;
    }

    public Map<Date, Integer> getTweetsVolume() {
        return tweetsVolume;
    }

    /**
     * @param args the command line arguments
     */
    public List<TweetVolumes> executeQuery() throws FileNotFoundException,
            UnsupportedEncodingException, IOException, ParseException {
        double startTime = System.currentTimeMillis();
        createStreamer();
        Map<Date, String> index = getIndexArmy();
        System.out.println("#number of dates found: " + index.size());
        Iterator it = index.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            System.out.println("#Start Reading index of " + entry.getKey().toString());
            int count = 0;
            count = GetSmartOutput(entry.getKey().toString(),entry.getValue().toString());

            dayVolume.add(new TweetVolumes((Date) entry.getKey(), count));

        }

        double endTime = System.currentTimeMillis();
        System.out.println("query time = " + (endTime - startTime) + " ms");
        closeStreamer();

        return dayVolume;

    }
    
    public GridCell readMastersFile() throws UnsupportedEncodingException, FileNotFoundException, IOException, ParseException{
    	
         Map<Date, String> index = getIndexArmy();
         System.out.println("#number of dates found: " + index.size());
         Iterator it = index.entrySet().iterator();
         long count = 0;
         GridCell cell = new GridCell(this.serverRequest.getMbr());
         while (it.hasNext()) {
             Map.Entry entry = (Map.Entry) it.next();
             System.out.println("#Start Reading index of " + entry.getKey().toString());
             List<Partition> file = ReadMaster(entry.getKey().toString(),entry.getValue().toString()+"/");
             for(Partition p : file){
            	 cell.add(p.getDay(), p.getCardinality());
             }

         }
         return cell;
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
                    serverRequest.getQuery(), Integer.MAX_VALUE);
            //SpatialFilter to the documents
            for (String doc : documents) {
                Tweet tweet = new Tweet(doc);
                if (serverRequest.getMbr().insideMBR(new Point(tweet.lat, tweet.lon))) {
                    outwriter.write(doc);
                    outwriter.write("\n");
                    count++;
                    insertTweetsToVolume(tweet.created_at);
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
     * This method check if the request for tweets or hashtags
     *
     * @param type
     * @param start
     * @param end
     * @return
     * @throws ParseException
     */
    private static Map<Date, String> getIndexArmy() throws ParseException, IOException {
        if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
            return lookup.getTweetsDayIndex(serverRequest.getStartDate(),
                    serverRequest.getEndDate());
        } else {
            return lookup.getHashtagDayIndex(serverRequest.getStartDate(),
                    serverRequest.getEndDate());
        }
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
//        args = new  String[11];
//        args[0] = "/home/turtle/UQUGIS/taghreed/Tools/twittercrawlermavenproject/output/result/";//System.getProperty("user.dir") + "/data/result";
//        args[1] = System.getProperty("user.dir")+"/export/";
//        args[2] = "180";//"21.509878763366647";//jedah
//        args[3] = "180";//"39.206080107128436";
//        args[4] = "-180";//"21.473700876235167";
//        args[5] = "-180";//"39.15913072053149";
//        args[6] = "2013-12-01";
//        args[7] = "2013-12-01";
//        args[8] = "tweet";//"tweet";
//        args[9] = "multi";
//        args[10] = "";
//        String folderPath = args[0] + "/";
//        lookup.loadLookupTableToArrayList(folderPath);
//        List<TweetVolumes> result = org.gistic.taghreed.diskBaseQuery.query.DayQueryProcessor.run(args);
//        Collections.sort(result);
//        for (int i = 0; i < result.size(); i++) {
//            System.out.println(result.get(i).dayName + "-" + result.get(i).volume);
//        }
    }
}
