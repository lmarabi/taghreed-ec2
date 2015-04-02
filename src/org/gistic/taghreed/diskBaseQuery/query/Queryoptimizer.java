/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.query;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.gistic.taghreed.Commons;
import org.gistic.taghreed.collections.TopTweetResult;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.collections.TweetVolumes;
import org.gistic.taghreed.collections.Week;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQueryOptimizer.GridCell;
import org.gistic.taghreed.spatialHadoop.Tweets;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import edu.umn.cs.spatialHadoop.operations.RangeQuery.RangeQueryMap;
import edu.umn.cs.spatialHadoop.osm.OSMPolygon;

/**
 *
 * @author turtle
 */
public class Queryoptimizer {
    //Global lookup
    private static Lookup lookup = new Lookup();
    public static PyramidQueryProcessor pyramidRequest;
    public static QueryExecutor dayrequest;
    public static List<TweetVolumes> dayVolume;
    private static ServerRequest serverRequest;
    private static Commons conf;

    public Queryoptimizer(ServerRequest serverRequest) throws IOException, FileNotFoundException, ParseException {
        this.serverRequest = serverRequest;
        pyramidRequest = new PyramidQueryProcessor(serverRequest);
        dayrequest = new QueryExecutor(serverRequest);
        lookup = serverRequest.getLookup();
        conf = new Commons();

    }

    /**
     * @param args the command line arguments
     * @throws InterruptedException 
     */
    public long executeQuery() throws FileNotFoundException,
            UnsupportedEncodingException, IOException, ParseException, InterruptedException {
        boolean queryTail = false;
        List<RunningJob> rangeJobs = new ArrayList<RunningJob>();
        Rectangle mbr = new Rectangle(serverRequest.getMbr().getMin().getLat(),
				serverRequest.getMbr().getMin().getLon(), serverRequest
						.getMbr().getMax().getLat(), serverRequest.getMbr()
						.getMax().getLon());
        double startTime = System.currentTimeMillis();
        //Query From Months
        Map<String, String> indexMonths = lookup.getTweetsMonthsIndex(serverRequest.getStartDate(), serverRequest.getEndDate());
        List<String> months = new ArrayList<String>();
        System.out.println("#number of Months found: " + indexMonths.size());
        Iterator it = indexMonths.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            System.out.println("#Start Reading index of " + entry.getKey().toString());
            months.add(entry.getKey().toString());
            rangeJobs.add(executeRangeQuery(mbr, getindexPath(entry.getKey().toString(),queryLevel.Month)));
        }
            Map<Week, String> index = lookup.getTweetsWeekIndex(serverRequest.getStartDate(),serverRequest.getEndDate(),months);
            System.out.println("#number of Weeks found: " + index.size());
            List<String> weeks = new ArrayList<String>();
            it = index.entrySet().iterator(); 
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                Week week = (Week) entry.getKey();
                System.out.println("#Start Reading Week " + week.getWeekName());
                weeks.add(week.getWeekName());
                rangeJobs.add(executeRangeQuery(mbr, getindexPath(entry.getKey().toString(),queryLevel.Week)));
            }
            //Query missing days in head week
            Map<String, String> indexDays = lookup.getTweetsDayIndex(serverRequest.getStartDate(),serverRequest.getEndDate(),weeks,months);
            System.out.println("#number of Days found: " + indexDays.size());
            it = indexDays.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                System.out.println("#Start Reading index of " + entry.getKey().toString());
                rangeJobs.add(executeRangeQuery(mbr, getindexPath(entry.getKey().toString(),queryLevel.Day)));
            }
         
        

        for(RunningJob j : rangeJobs){
        	j.waitForCompletion();
        }
        double endTime = System.currentTimeMillis();
        System.out.println("query time = " + (endTime - startTime) + " ms");
        return 0;

    }
    
    private String getindexPath(String index,queryLevel level){
    	return this.conf.getHadoopHDFSPath()+level.toString()+"/index."+index;
    }
    
    /**
	 * This method execute the range query from the disk. 
	 * @param mbr
	 * @param path
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private static RunningJob executeRangeQuery(Rectangle mbr, String path)
			throws IllegalArgumentException, IOException {
		long count;
		OperationsParams operationsParams = new OperationsParams();
		operationsParams.setClass("shape", Tweets.class, Shape.class);
		operationsParams.setShape(operationsParams, "rect", mbr);
		RunningJob job = RangeQuery.rangeQueryMapReduce(new Path(path), null, operationsParams);
		return job;
	}
    
    

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException, IOException, ParseException {
//        args = new String[11];
//        args[0] = "/home/turtle/UQUGIS/taghreed/Tools/twittercrawlermavenproject/output/result/";//System.getProperty("user.dir") + "/data/result";
//        args[1] = System.getProperty("user.dir") + "/export/";
//        args[2] = "180";//"21.509878763366647";//jedah
//        args[3] = "180";//"39.206080107128436";
//        args[4] = "-180";//"21.473700876235167";
//        args[5] = "-180";//"39.15913072053149";
//        args[6] = "2013-10-01";
//        args[7] = "2013-12-01";
//        args[8] = "tweet";
//        args[9] = "multi";
//        args[10] = "";
//        String folderPath = args[0] + "/";
//        lookup.loadLookupTableToArrayList(folderPath);
//        List<TweetVolumes> result = org.gistic.taghreed.diskBaseQuery.query.Queryoptimizer.run(args);
//        Collections.sort(result);
//        for (int i = 0; i < result.size(); i++) {
//            System.out.println(result.get(i).dayName + "-" + result.get(i).volume);
//        }
    }
}
