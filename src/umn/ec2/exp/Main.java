package umn.ec2.exp;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import org.gistic.taghreed.diskBaseIndexer.MainBackendIndex;
import org.gistic.taghreed.diskBaseQuery.query.Queryoptimizer;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryIndex;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryType;

public class Main {
	static String globalStartDate = "2013-10-01";
	static String globalEndDate = "2015-03-31";

	public static void main(String[] args) throws Exception {
		// args = new String[2];
		// args[0] = "query";
		// args[1] = "spatial";
		// TODO Auto-generated method stub
		if (args.length == 2) {
			String operation = args[0];
			String level = args[1];
			if (operation.equals("index")) {
				System.out.println("Indexing operation is running Now");
				OutputStreamWriter writer = new OutputStreamWriter(
						new FileOutputStream(System.getProperty("user.dir")
								+ "/" + "IndexOperation_time.log", true),
						"UTF-8");
				MainBackendIndex indexOp = new MainBackendIndex();
				Responder respondHandler = new Responder();
				indexOp.setHandler(respondHandler);
				if (level.equals("day")) {
					indexOp.indexDayLevel();
				} else if (level.equals("week")) {
					indexOp.indexWeekLevel();
				} else if (level.equals("month")) {
					indexOp.indexMonthLevel();
				} else {
					indexOp.indexWholeOneIndex();
				}
				System.out.println("Total Indexing Time (" + level
						+ ") ********** "
						+ respondHandler.getTotalExecutionTimes());
				writer.write("\n" + level + ","
						+ respondHandler.getTotalExecutionTimes());
				writer.close();

			}
		} else if (args.length == 3) {
			String operation = args[0];
			String parameter = args[1];
			int startDay = Integer.parseInt(args[2]);
			if (operation.equals("query")) {
				System.out.println("Query operation is running Now");
				RangeQueryExperiments(parameter, startDay);
			}

		} else {
			System.out
					.println("To use this program you must pass the following arguments\n*********\n"
							+ "index [level(day,week,month)]\n"
							+ "query [level(day,week,month)]\n"
							+ "query [spatial|temporal]\tstartDay - int number only for example 1 or 13\n"
							+ "query whole [day|week|month|all]\n");
		}
	}

	private static void RangeQueryExperiments(String parameter, int startDay)
			throws Exception {
		if (parameter.equals("spatial")) {
			spatialRangeQueryExpr(startDay);
		} else if (parameter.equals("temporal")) {
			temporalRangeQueryExpr(startDay);
		} else {
			if(parameter.equals("day")){
				spatialRangeQueryDays(startDay);
			}else if(parameter.equals("week")){
				spatialRangeQueryWeek(startDay);
			}else if(parameter.equals("month")){
				spatialRangeQueryMonth(startDay);
			}else{
				spatialRangeQueryWhole(startDay);
			}
		}
	}

	/***
	 * In this method we change the spatial Range of the query And fix the
	 * following: Temporal , And Query Processing.
	 * 
	 * @throws Exception
	 */
	private static void spatialRangeQueryExpr(int startDay) throws Exception {
		// double[] area = {0.0001,0.001,0.01};
		double[] area = { 0.000001, 0.0001, 0.01, 0.1 };
		for (int i = 0; i < area.length; i++) {

			SamplersCollector sampleHandler = new SamplersCollector();
			ServerRequest req = new ServerRequest();
			req.setStartDate("2014-05-01");
			req.setEndDate("2014-05-31");
			req.setType(queryType.tweet);
			req.setIndex(queryIndex.rtree);
			req.setNumSamples(20);
			/*
			 * Read A sample from Index
			 */
			Queryoptimizer queryExec = new Queryoptimizer(req);
			queryExec.setExpName("TakeSamples");
			queryExec.addSampleHandler(sampleHandler);
			queryExec.readSamplesMBR();
			req.setRect(sampleHandler.getSamples(), area[i]);
			/*
			 * Now Execute the Range Query
			 */

			String startTime = "2014-05-01";
			String endTime = "2014-05-";
			queryExec = new Queryoptimizer(req);
			queryExec.setSpatialRatio(area[i]);
			queryExec.setExpName("RangeQueryExp_Spatial");
			for (int d = startDay; d < 32; d++) {
				if (d == 1 || d == 3 || d == 4 || d == 10 || d == 11 || d == 17
						|| d == 18 || d == 24 || d == 25 | d == 31) {
					if (d < 10) {
						endTime = "2014-05-0" + d;
					} else {
						endTime = "2014-05-" + d;
					}
					req.setStartDate(startTime);
					req.setEndDate(endTime);
					try {
						queryExec.executeQuery();
					} catch (Exception ex) {

					}

				}
			}
		}

	}

	/***
	 * In this method we change the spatial Range of the query And fix the
	 * following: Temporal , And Query Processing.
	 * 
	 * @throws Exception
	 */
	private static void spatialRangeQueryDays(int startDay) throws Exception {
		// double[] area = {0.0001,0.001,0.01};
		double[] area = { 0.000001, 0.0001, 0.01, 0.1 };
		for (int i = 0; i < area.length; i++) {

			SamplersCollector sampleHandler = new SamplersCollector();
			ServerRequest req = new ServerRequest();
			req.setStartDate(globalStartDate);
			req.setEndDate(globalEndDate);
			req.setType(queryType.tweet);
			req.setIndex(queryIndex.rtree);
			req.setNumSamples(1);
			/*
			 * Read A sample from Index
			 */
			Queryoptimizer queryExec = new Queryoptimizer(req);
			queryExec.setExpName("TakeSamples");
			queryExec.addSampleHandler(sampleHandler);
			//queryExec.readSamplesMBR();
			req.setRect(sampleHandler.getSamples(), area[i]);
			/*
			 * Now Execute the Range Query
			 */
			queryExec = new Queryoptimizer(req);
			queryExec.setSpatialRatio(area[i]);
			queryExec.setExpName("RangeQueryExp_Spatial_day"+area[i]);
			try {
				queryExec.executeDayLevelOnly();
			} catch (Exception ex) {

			}

		}

	}
	
	
	/***
	 * In this method we change the spatial Range of the query And fix the
	 * following: Temporal , And Query Processing.
	 * 
	 * @throws Exception
	 */
	private static void spatialRangeQueryWeek(int startDay) throws Exception {
		// double[] area = {0.0001,0.001,0.01};
		double[] area = { 0.000001, 0.0001, 0.01, 0.1 };
		for (int i = 0; i < area.length; i++) {

			SamplersCollector sampleHandler = new SamplersCollector();
			ServerRequest req = new ServerRequest();
			req.setStartDate(globalStartDate);
			req.setEndDate(globalEndDate);
			req.setType(queryType.tweet);
			req.setIndex(queryIndex.rtree);
			req.setNumSamples(1);
			/*
			 * Read A sample from Index
			 */
			Queryoptimizer queryExec = new Queryoptimizer(req);
			queryExec.setExpName("TakeSamples");
			queryExec.addSampleHandler(sampleHandler);
			//queryExec.readSamplesMBR();
			req.setRect(sampleHandler.getSamples(), area[i]);
			/*
			 * Now Execute the Range Query
			 */
			queryExec = new Queryoptimizer(req);
			queryExec.setSpatialRatio(area[i]);
			queryExec.setExpName("RangeQueryExp_Spatial_week"+area[i]);
			try {
				queryExec.executeWeekLevelOnly();
			} catch (Exception ex) {

			}

		}

	}
	
	
	/***
	 * In this method we change the spatial Range of the query And fix the
	 * following: Temporal , And Query Processing.
	 * 
	 * @throws Exception
	 */
	private static void spatialRangeQueryMonth(int startDay) throws Exception {
		// double[] area = {0.0001,0.001,0.01};
		double[] area = { 0.000001, 0.0001, 0.01, 0.1 };
		for (int i = 0; i < area.length; i++) {

			SamplersCollector sampleHandler = new SamplersCollector();
			ServerRequest req = new ServerRequest();
			req.setStartDate(globalStartDate);
			req.setEndDate(globalEndDate);
			req.setType(queryType.tweet);
			req.setIndex(queryIndex.rtree);
			req.setNumSamples(1);
			/*
			 * Read A sample from Index
			 */
			Queryoptimizer queryExec = new Queryoptimizer(req);
			queryExec.setExpName("TakeSamples");
			queryExec.addSampleHandler(sampleHandler);
			//queryExec.readSamplesMBR();
			req.setRect(sampleHandler.getSamples(), area[i]);
			/*
			 * Now Execute the Range Query
			 */
			queryExec = new Queryoptimizer(req);
			queryExec.setSpatialRatio(area[i]);
			queryExec.setExpName("RangeQueryExp_Spatial_month"+area[i]);
			try {
				queryExec.executeMonthLevelOnly();
			} catch (Exception ex) {

			}

		}

	}
	
	/***
	 * In this method we change the spatial Range of the query And fix the
	 * following: Temporal , And Query Processing.
	 * 
	 * @throws Exception
	 */
	private static void spatialRangeQueryWhole(int startDay) throws Exception {
		// double[] area = {0.0001,0.001,0.01};
		double[] area = { 0.000001, 0.0001, 0.01, 0.1 };
		for (int i = 0; i < area.length; i++) {

			SamplersCollector sampleHandler = new SamplersCollector();
			ServerRequest req = new ServerRequest();
			req.setStartDate(globalStartDate);
			req.setEndDate(globalEndDate);
			req.setType(queryType.tweet);
			req.setIndex(queryIndex.rtree);
			req.setNumSamples(1);
			/*
			 * Read A sample from Index
			 */
			Queryoptimizer queryExec = new Queryoptimizer(req);
			queryExec.setExpName("TakeSamples");
			queryExec.addSampleHandler(sampleHandler);
			//queryExec.readSamplesMBR();
			req.setRect(sampleHandler.getSamples(), area[i]);
			/*
			 * Now Execute the Range Query
			 */
			queryExec = new Queryoptimizer(req);
			queryExec.setSpatialRatio(area[i]);
			queryExec.setExpName("RangeQueryExp_Spatial_Whole"+area[i]);
			try {
				queryExec.executeAllIndex();
			} catch (Exception ex) {

			}

		}

	}
	

	/***
	 * In this method we change the temporal Range of the query And fix the
	 * following: spatial, and query processing technique
	 * 
	 * @throws Exception
	 */
	private static void temporalRangeQueryExpr(int startDay) throws Exception {
		SamplersCollector sampleHandler = new SamplersCollector();
		ServerRequest req = new ServerRequest();
		req.setStartDate("2014-05-01");
		req.setEndDate("2014-09-30");
		req.setType(queryType.tweet);
		req.setIndex(queryIndex.rtree);
		// double maxlon = -93.18933596240234;
		// double minlat = 44.94941027490235;
		// double maxlat = 45.01670153466797;
		// double minlon = -93.3176528416748;
		// MBR mbr = new MBR(new Point(maxlat, maxlon), new Point(minlat,
		// minlon));
		// req.setMBR(mbr);
		req.setNumSamples(20);
		/*
		 * Read A sample from Index
		 */
		Queryoptimizer queryExec = new Queryoptimizer(req);
		queryExec.setExpName("TakeSamples");
		queryExec.addSampleHandler(sampleHandler);
		queryExec.readSamplesMBR();
		req.setRect(sampleHandler.getSamples(), (double) 0.0001);
		/*
		 * Now Execute the Range Query
		 */

		String startTime = "2014-05-01";
		queryExec = new Queryoptimizer(req);
		queryExec.setSpatialRatio((double) 0.0001);
		queryExec.setExpName("RangeQueryExp_temporal");
		String[] endTime = { "2014-05-31", "2014-06-15", "2014-06-30",
				"2014-07-15", "2014-07-31", "2014-08-15", "2014-08-31",
				"2014-09-15", "2014-09-30" };
		for (int i = 0; i < endTime.length; i++) {
			req.setStartDate(startTime);
			req.setEndDate(endTime[i]);
			try {
				queryExec.executeQuery();
			} catch (Exception ex) {

			}
		}

	}

}
