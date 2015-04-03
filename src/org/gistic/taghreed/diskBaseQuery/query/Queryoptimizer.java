/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.query;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gistic.taghreed.Commons;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.collections.TweetVolumes;
import org.gistic.taghreed.collections.Week;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;

import umn.ec2.exp.Initiater;
import umn.ec2.exp.Responder;
import edu.umn.cs.spatialHadoop.core.Rectangle;

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
    private static Initiater trigger;
    private static String expName;

    public Queryoptimizer(ServerRequest serverRequest) throws IOException, FileNotFoundException, ParseException {
        this.serverRequest = serverRequest;
        pyramidRequest = new PyramidQueryProcessor(serverRequest);
        dayrequest = new QueryExecutor(serverRequest);
        lookup = serverRequest.getLookup();
        conf = new Commons();
        this.trigger = new Initiater();
        this.expName = "";
    }
    
    
    public void setExpName(String Name){
    	this.expName = Name;
    }
    
    public void addHandler(Responder handler){
    	this.trigger.addListener(handler);
    }

    /**
     * @param args the command line arguments
     * @throws InterruptedException 
     */
    public long executeQuery() throws FileNotFoundException,
            UnsupportedEncodingException, IOException, ParseException, InterruptedException {
        boolean queryTail = false;
        List<Thread> threads = new ArrayList<Thread>();
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
           threads.add(executeRangeQuery(serverRequest.getMbr(), entry.getKey().toString(),queryLevel.Month));
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
                threads.add(executeRangeQuery(serverRequest.getMbr(),entry.getKey().toString(),queryLevel.Week));
            }
            //Query missing days in head week
            Map<String, String> indexDays = lookup.getTweetsDayIndex(serverRequest.getStartDate(),serverRequest.getEndDate(),weeks,months);
            System.out.println("#number of Days found: " + indexDays.size());
            it = indexDays.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                System.out.println("#Start Reading index of " + entry.getKey().toString());
                threads.add(executeRangeQuery(serverRequest.getMbr(), entry.getKey().toString(),queryLevel.Day));
            }
         
        for(Thread t : threads){
        	t.join();
        }
        
        double endTime = System.currentTimeMillis();
        System.out.println("query time = " + (endTime - startTime) + " ms");
        return 0;

    }
    
    private String getCommand(MBR mbr,String index,queryLevel level){
    	String ec2AccessCode = this.conf.getEc2AccessCode();
    	String indexDir=  this.conf.getHadoopHDFSPath()+level.toString()+"/index."+index;
    	String shape = "org.gistic.taghreed.spatialHadoop.Tweets";
    	Rectangle rectangle = new Rectangle(mbr.getMin().getLat(),
    			mbr.getMin().getLon(), mbr.getMax().getLat(), mbr.getMax().getLon());
    	String rect =  "rect:"+rectangle.x1+","+rectangle.y1+","+rectangle.x2+","+rectangle.y2;
    	String cmd =  this.conf.getHadoopDir()+"hadoop jar " + this.conf.getShadoopJar() +" rangequery "+ "-libjars "+this.conf.getLibJars()+" "+ec2AccessCode+" "+indexDir+" "+rect+" shape:"+shape+"  -no-local";
    	return cmd;
    }
    
    /**
	 * This method execute the range query from the disk. 
	 * @param mbr
	 * @param path
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private Thread executeRangeQuery(MBR mbr, String index,queryLevel level) {
		String command = getCommand(mbr,index,level);
		Thread t = new Thread(new rangeQueryHDFS(command));
		t.start();
		return t;
		
	}
	
	public static void commandExecuter(String command) throws IOException,
			InterruptedException {
		System.out.println(command);
		Process myProcess = Runtime.getRuntime().exec(command);

		StreamGobbler errorGobbler = new StreamGobbler(
				myProcess.getErrorStream(), System.out,trigger,expName);

		// Any output?
		StreamGobbler outputGobbler = new StreamGobbler(
				myProcess.getInputStream(), System.err,trigger,expName);

		errorGobbler.start();
		outputGobbler.start();

		// Any error
		int exitVal = myProcess.waitFor();
		errorGobbler.join(); // Handle condition where the
		outputGobbler.join(); // process ends before the threads finish
	}
	
	
	class rangeQueryHDFS implements Runnable{
		String command;
		
		
		public rangeQueryHDFS(String command) {
			this.command = command;
		}
		
		@Override
		public void run() {
			try {
				commandExecuter(command);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
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


class StreamGobbler extends Thread {
	InputStream is;
	PrintStream os;
	OutputStreamWriter writer;
	Initiater trigger;

	StreamGobbler(InputStream is, PrintStream os,Initiater trigger,String exprName) throws UnsupportedEncodingException, FileNotFoundException {
		this.is = is;
		this.os = os;
		this.writer = new OutputStreamWriter(
				new FileOutputStream(
						System.getProperty("user.dir")
								+ "/"+exprName+".log", true),
				"UTF-8");
		this.trigger = trigger;
		
	}

	public void run() {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(is));
			 String line = null;
			 while ((line = reader.readLine()) != null) {
				 os.println(line);
				 writer.write(line+"\n");
				 writer.flush();
				 if(line.contains("Time")){
					 this.trigger.notifyExecutionTime(line);
				 }
			 }
			 writer.close();
		}catch (IOException ioe) {
			//handel error.
		}
		
	}
}
