package umn.ec2.exp;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gistic.taghreed.diskBaseIndexer.MainBackendIndex;
import org.gistic.taghreed.diskBaseQuery.query.Queryoptimizer;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryIndex;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryType;

import com.sun.rmi.rmid.ExecPermission;

public class Main {

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
		} else if (args.length == 4) {
			String operation = args[0];
			String parameter = args[1];
			String startDay = args[2];
			String endDay = args[3];
			if (operation.equals("query")) {
				System.out.println("Query operation is running Now");
				RangeQueryExperiments(parameter, startDay, endDay);
			}

		} else {
			System.out
					.println("To use this program you must pass the following arguments\n*********\n"
							+ "index [level(day,week,month)]\n"
							+ "query [spatial|temporal] startDate endDate\t spatial constains all ratios while temporal contains only the default");
		}
	}

	private static void RangeQueryExperiments(String parameter,
			String startDay, String endDay) throws Exception {
		if (parameter.equals("spatial")) {
			spatialRangeQueryExpr(startDay, endDay);
		} else if (parameter.equals("temporal")) {
			temporalRangeQueryExpr(startDay, endDay);
		} else {
			// this change the query execution techniques.
		}
	}

	/***
	 * In this method we change the spatial Range of the query And fix the
	 * following: Temporal , And Query Processing.
	 * 
	 * @throws Exception
	 */
	private static void spatialRangeQueryExpr(String startDay, String endDay)
			throws Exception {
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
			queryExec = new Queryoptimizer(req);
			queryExec.setSpatialRatio(area[i]);
			queryExec.setExpName("RangeQueryExp_Spatial");
			Map<String, String> days = req.getLookup().getTweetsDayIndex(
					startDay, endDay);
			List<String> sortedDays = new ArrayList<String>();
			Iterator it = days.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = (Map.Entry) it.next();
				sortedDays.add(entry.getKey());
			}
			Collections.sort(sortedDays);
			sortedDays.remove(0);
			for (String end : sortedDays) {
				req.setStartDate(startDay);
				req.setEndDate(end);
				try {
					queryExec.executeQuery();
				} catch (Exception ex) {

				}
			}

		}

	}

	/***
	 * In this method we change the temporal Range of the query And fix the
	 * following: spatial, and query processing technique
	 * 
	 * @throws Exception
	 */
	private static void temporalRangeQueryExpr(String startDay, String endDay)
			throws Exception {
		SamplersCollector sampleHandler = new SamplersCollector();
		ServerRequest req = new ServerRequest();
		req.setStartDate("2014-05-01");
		req.setEndDate("2014-05-31");
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
		String endTime = "2014-05-";
		queryExec = new Queryoptimizer(req);
		queryExec.setSpatialRatio((double) 0.0001);
		queryExec.setExpName("RangeQueryExp_temporal");
		Map<String, String> days = req.getLookup().getTweetsDayIndex(startDay,
				endDay);
		List<String> sortedDays = new ArrayList<String>();
		Iterator it = days.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = (Map.Entry) it.next();
			sortedDays.add(entry.getKey());
		}
		Collections.sort(sortedDays);
		sortedDays.remove(0);
		for (String end : sortedDays) {
			req.setStartDate(startDay);
			req.setEndDate(end);
			try {
				queryExec.executeQuery();
			} catch (Exception ex) {

			}
		}

	}

}
