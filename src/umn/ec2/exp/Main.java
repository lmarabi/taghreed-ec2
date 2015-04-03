package umn.ec2.exp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;

import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.diskBaseIndexer.MainBackendIndex;
import org.gistic.taghreed.diskBaseQuery.query.Queryoptimizer;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryIndex;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryType;

import Pyramid.QueryProcessor;

public class Main {


	public static void main(String[] args) throws IOException,
			InterruptedException, ParseException {
		args = new String[2];
		args[0] = "query";
		args[1] = "temporal";
		// TODO Auto-generated method stub
		if (args.length == 2) {
			String operation = args[0];
			String level = args[1];
			if (operation.equals("index")) {
				System.out.println("Indexing operation is running Now");
				MainBackendIndex indexOp = new MainBackendIndex();
				if (level.equals("day")) {
					indexOp.indexDayLevel();
				} else if (level.equals("week")) {
					indexOp.indexWeekLevel();
				} else {
					indexOp.indexMonthLevel();
				}

			} else if (operation.equals("query")) {
				System.out.println("Query operation is running Now");
				RangeQueryExperiments(level);
			}
		} else {
			System.out
					.println("To use this program you must pass the following arguments\n*********\n"
							+ "index [level(day,week,month)]\n"
							+ "query [level(day,week,month)]\n"
							+ "query temporal\t this is will query from all levels");
		}
	}
	
	private static void RangeQueryExperiments(String parameter) throws FileNotFoundException, IOException, ParseException, InterruptedException{
		if(parameter.equals("spatial")){
			spatialRangeQueryExpr();
		}else if(parameter.equals("temporal")){
			temporalRangeQueryExpr();
		}else{
			// this change the query execution techniques. 
		}
	}
	
	/***
	 * In this method we change the spatial Range of the query
	 * And fix the following: Temporal , And Query Processing.
	 */
	private static void spatialRangeQueryExpr(){
		
	}
	
	/***
	 * In this method we change the temporal Range of the query
	 * And fix the following: spatial, and query processing technique
	 * @throws ParseException 
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws InterruptedException 
	 */
	private static void temporalRangeQueryExpr() throws FileNotFoundException, IOException, ParseException, InterruptedException{
		Responder respondHandler = new Responder();
		ServerRequest req = new ServerRequest();
		req.setStartDate("2014-03-01");
		req.setEndDate("2014-04-27");
		req.setType(queryType.tweet);
		req.setIndex(queryIndex.rtree);
		double maxlon = -93.18933596240234;
		double minlat = 44.94941027490235;
		double maxlat = 45.01670153466797;
		double minlon = -93.3176528416748;
		MBR mbr = new MBR(new Point(maxlat, maxlon), new Point(minlat, minlon));
		req.setMBR(mbr);
		Queryoptimizer queryExec = new Queryoptimizer(req);
		queryExec.addHandler(respondHandler);
		queryExec.setExpName("temporalExp1");
		queryExec.executeQuery();
		System.out.println("Main>>>> "+respondHandler.getExecutionTimes());
		
		
	}

}
