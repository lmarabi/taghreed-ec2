package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;

import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryIndex;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryType;

/**
 * This grid preprocessed to create the final grids for Day, week , Months
 * 
 * @author louai
 *
 */
public class Grid {
	String startDay;
	String endDay;
	int LonDomain;
	int LatDomain;
	GridCell[][] cells;
	queryLevel level;

	/**
	 * @param StartDay
	 *            , EndDay Dayformat yyyy-MM-DD queryLevel is
	 *            Enum{Day,Week,Month}
	 */
	public Grid(String startDay, String endDay, queryLevel queryLevel) {
		this.startDay = startDay;
		this.endDay = endDay;
		this.LonDomain = 360;
		this.LatDomain = 180;
		this.cells = new GridCell[LonDomain][LatDomain];
		this.level = queryLevel;
	}

	public void BuildGrid() throws FileNotFoundException, IOException,
			ParseException {
		System.out.println("Start building " + level.toString() + " ... ");
		double startTime = System.currentTimeMillis();
		// init writers
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir") + "/_Grid_"
						+ this.level.toString() + ".txt", false), "UTF-8");
		OutputStreamWriter WKTwriter = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir") + "/_Grid_"
						+ level.toString() + ".WKT", false), "UTF-8");
		OutputStreamWriter writerNotfound = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir") + "/_Grid_"
						+ level.toString() + ".miss", false), "UTF-8");
		writerNotfound.write("Start building " + level.toString() + " ... ");
		WKTwriter
				.write("id\tpolygonShape\tAverage\tStandard Deviation\tRelative Deviation\tStandard Error\tRelative Error\n");
		int counter = 1;
		// iterate the grid index
		ServerRequest req = new ServerRequest();
		req.setIndex(queryIndex.rtree);
		req.setType(queryType.tweet);
		req.setStartDate(startDay);
		req.setEndDate(endDay);
		GridCell cell;
		MBR mbr;
		Point min;
		Point max;
		for (int lat = 0; lat < LatDomain; lat++) {
			for (int lon = 0; lon < LonDomain; lon++) {
				min = new Point((lat - 90), (lon - 180));
				max = new Point((lat - 90 + 1), (lon - 180 + 1));
				mbr = new MBR(max, min);
				cell = new GridCell(mbr, req.getLookup());
				System.out.println(mbr.toString());
				req.setMBR(mbr);
				// cells[lon][lat] = req.getMasterRtreeDays(this.level);

				cell = req.getMasterRtreeDays(this.level);
				// write the result only if the cell is not empty
				if (cell.getSampleSize() != 0) {
					// Write grid data.
					writer.write(lon + "," + lat + "," + cell.toString() + "\n");
					// write WKT
					WKTwriter.write(counter + "\t" + cell.getMbr().toWKT()
							+ "\t" + cell.getAverage() + "\t"
							+ cell.getStandardDeviation() + "\t"
							+ cell.getStandardRelativeDeviation() + "\t"
							+ cell.getStandardError() + "\t"
							+ cell.getStandardRelativeError() + "\n");
					counter++;
				} else {
					System.err.println("mbr not found" + mbr.toString());
					writerNotfound.write(lon + "," + lat + "," + mbr.toString()
							+ "\n");
				}

			}
		}
		double endTime = System.currentTimeMillis();
		System.out.println("Time to Build: " + (endTime - startTime)
				+ " Millis");
		writerNotfound.write("Time to Build: " + (endTime - startTime)
				+ " Millis");
		writer.close();
		WKTwriter.close();
		writerNotfound.close();

	}

	/***
	 * This mehod write the grid to the disk
	 * 
	 * @throws IOException
	 */
	public void writeGridToDisk() throws IOException {
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir") + "/_Grid_"
						+ this.level.toString() + ".txt", true), "UTF-8");
		for (int lon = 0; lon < LonDomain; lon++) {
			for (int lat = 0; lat < LatDomain; lat++) {
				writer.write(lon + "," + lat + "," + cells[lon][lat].toString()
						+ "\n");
			}
		}
		writer.close();

	}

	/**
	 * This methdo write the grid in Well Known Text (WKT) format
	 * 
	 * @throws IOException
	 * @throws ParseException
	 */
	public void writeGridToKWT() throws IOException, ParseException {
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir") + "/_Grid_"
						+ level.toString() + ".WKT", false), "UTF-8");
		int counter = 1;
		writer.write("id\tpolygonShape\tAverage\tStandard Deviation\tStandard Error\n");
		for (int lon = 0; lon < LonDomain; lon++) {
			for (int lat = 0; lat < LatDomain; lat++) {
				writer.write(counter + "\t" + cells[lon][lat].getMbr().toWKT()
						+ "\t" + cells[lon][lat].getAverage() + "\t"
						+ cells[lon][lat].getStandardDeviation() + "\t"
						+ cells[lon][lat].getStandardError() + "\n");
				counter++;
			}
		}
		writer.close();
	}

	/**
	 * This method will read the stored Grid from the disk
	 * 
	 * @throws IOException
	 * @throws ParseException
	 */
	public void readGridFromDisk() throws IOException, ParseException {
		BufferedReader reader = new BufferedReader(new FileReader(
				System.getProperty("user.dir") + "/_Grid_"
						+ this.level.toString() + ".txt"));
		ServerRequest req = new ServerRequest();
		req.setIndex(queryIndex.rtree);
		req.setType(queryType.tweet);
		String line;
		int lon, lat;
		while ((line = reader.readLine()) != null) {
			String[] token = line.split(",");
			lon = Integer.parseInt(token[0]);
			lat = Integer.parseInt(token[1]);
			GridCell temp = new GridCell(token[2], req.getLookup());
			this.cells[lon][lat] = temp;
		}
		reader.close();
	}

	

	/**
	 * This method create a cluster in histogram, the threshold from 0-1 which
	 * represent the confidence level of the estimated cardinality.
	 * 
	 * @param confidenceThreashold
	 * @throws ParseException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void createClusters(double confidenceThreshold)
			throws ParseException, FileNotFoundException, IOException {
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir")
						+ "/_Histogram_" + this.level.toString() + ".txt",
						false), "UTF-8");
		boolean stop = false;
		for (int lon = 0; lon < LonDomain; lon++) {
			for (int lat = 0; lat < LatDomain; lat++) {
				System.out.println(lon + "," + lat);
				try {

					GridCell temp = cells[lon][lat];
					cells[lon][lat].initCluster(confidenceThreshold);
					writer.write(lon + "," + lat + ","
							+ cells[lon][lat].toStringHistogram() + "\n");
				} catch (NullPointerException e) {
					System.out.println(lon + "-" + lat);
				}
				// stop = true;
				// break;

			}
			// if(stop)
			// break;
		}
		writer.close();
	}

	

	private void buildWholeSpace() throws IOException, ParseException {
		System.out.println("Start building " + level.toString() + " ... ");
		double startTime = System.currentTimeMillis();
		// init writers
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir") + "/_Grid_"
						+ this.level.toString() + ".txt", false), "UTF-8");
		OutputStreamWriter WKTwriter = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir") + "/_Grid_"
						+ level.toString() + ".WKT", false), "UTF-8");
		OutputStreamWriter writerNotfound = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir") + "/_Grid_"
						+ level.toString() + ".miss", false), "UTF-8");
		writerNotfound.write("Start building " + level.toString() + " ... ");
		WKTwriter
				.write("id\tpolygonShape\tAverage\tStandard Deviation\tRelative Deviation\tStandard Error\tRelative Error\n");
		int counter = 1;
		// iterate the grid index
		ServerRequest req = new ServerRequest();
		req.setIndex(queryIndex.rtree);
		req.setType(queryType.tweet);
		req.setStartDate(startDay);
		req.setEndDate(endDay);
		GridCell cell;
		MBR mbr;
		Point min;
		Point max;
		min = new Point(-90, -180);
		max = new Point(90, 180);
		mbr = new MBR(max, min);
		cell = new GridCell(mbr, req.getLookup());
		System.out.println(mbr.toString());
		req.setMBR(mbr);
		// cells[lon][lat] = req.getMasterRtreeDays(this.level);
		cell = req.getMasterRtreeDays(this.level);
		// write the result only if the cell is not empty
		if (cell.getSampleSize() != 0) {
			// Write grid data.
			writer.write(0 + "," + 0 + "," + cell.toString() + "\n");
			// write WKT
			WKTwriter.write(counter + "\t" + cell.getMbr().toWKT() + "\t"
					+ cell.getAverage() + "\t" + cell.getStandardDeviation()
					+ "\t" + cell.getStandardRelativeDeviation() + "\t"
					+ cell.getStandardError() + "\t"
					+ cell.getStandardRelativeError() + "\n");
			counter++;
		} else {
			System.err.println("mbr not found" + mbr.toString());
			writerNotfound.write(mbr.toString());
		}

		double endTime = System.currentTimeMillis();
		System.out.println("Time to Build: " + (endTime - startTime)
				+ " Millis");
		writerNotfound.write("Time to Build: " + (endTime - startTime)
				+ " Millis");
		writer.close();
		WKTwriter.close();
		writerNotfound.close();
	}

	public static void main(String[] args) throws FileNotFoundException,
			IOException, ParseException {
		for (queryLevel q : queryLevel.values()) {
//			if (q.equals(queryLevel.Month) || q.equals(queryLevel.Week))
//				continue;
			System.out.println("************" + q.toString()
					+ "****************");
			Grid grid = new Grid("2012-01-01", "2015-10-30", q);
			System.out.println("Building Grid for " + q.toString());
//			 grid.BuildGrid();
			// grid.buildWholeSpace();
			// System.out.println("Writing Grid to Disk");
			// grid.writeGridToDisk();
			 grid.readGridFromDisk();
			 grid.createClusters(0.90);
			// System.out.println("Writing WKT to Disk");
			// // grid.writeGridToKWT();
		}
		System.out.println("Program done");
	}

}
