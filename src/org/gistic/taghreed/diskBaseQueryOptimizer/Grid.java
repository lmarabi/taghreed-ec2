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
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;

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
	 * @param StartDay, EndDay Dayformat yyyy-MM-DD
	 * queryLevel is Enum{Day,Week,Month}
	 */
	public Grid(String startDay, String endDay,queryLevel queryLevel) {
		this.startDay = startDay;
		this.endDay = endDay;
		this.LonDomain = 360;
		this.LatDomain = 180;
		this.cells = new GridCell[LonDomain][LatDomain];
		this.level = queryLevel;
	}

	public void BuildGrid() throws FileNotFoundException, IOException,
			ParseException {
		double startTime = System.currentTimeMillis();
		//init writers 
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir")
						+ "/_Grid_"+this.level.toString()+".txt", false), "UTF-8");
		OutputStreamWriter WKTwriter = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir")
						+ "/_Grid_"+level.toString()+".WKT", false), "UTF-8");
		WKTwriter.write("id\tpolygonShape\tAverage\tStandard Deviation\tStandard Error\n");
		int counter =1;
		//iterate the grid index
		ServerRequest req = new ServerRequest(1);
		for (int lat = 0; lat < LatDomain; lat++) {
			for (int lon = 0; lon < LonDomain; lon++) {
				Point min = new Point((lat - 90), (lon - 180));
				Point max = new Point((lat - 90+1), (lon - 180+1));
				MBR mbr = new MBR(max, min);
				System.out.println(mbr.toString());
				GridCell cell = new GridCell(mbr);
				System.out.println(mbr.toString());
				req.setMBR(String.valueOf(max.getLat()),
						String.valueOf(max.getLon()),
						String.valueOf(min.getLat()),
						String.valueOf(min.getLon()));
				req.setStartDate(startDay);
				req.setEndDate(endDay);
				//cells[lon][lat] = req.getMasterRtreeDays(this.level);
				cell = req.getMasterRtreeDays(this.level);
				//write the result only if the cell is not empty
				if(cell.getSampleSize() != 0){
					//Write grid data.
					writer.write(lon + "," + lat + "," + cell.toString()
							+ "\n");
					//write WKT
					WKTwriter.write(counter + "\t" + cell.getMbr().toWKT()
							+ "\t" + cell.getAverage() + "\t"
							+ cell.getStandardDeviation() + "\t"
							+ cell.getStandardError() + "\n");
					counter++;
				}
				
				
			}
		}
		double endTime = System.currentTimeMillis();
		System.out.println("Time to Build: "+(endTime-startTime)+" Millis");

	}

	/***
	 * This mehod write the grid to the disk
	 * 
	 * @throws IOException
	 */
	public void writeGridToDisk() throws IOException {
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir")
						+ "/_Grid_"+this.level.toString()+".txt", true), "UTF-8");
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
	 * @throws IOException 
	 */
	public void writeGridToKWT() throws IOException{
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(System.getProperty("user.dir")
						+ "/_Grid_"+level.toString()+".WKT", false), "UTF-8");
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

	public void readGridFromDisk() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				System.getProperty("user.dir") + "/_Grid_"+this.level.toString()+".txt"));
		String line;
		int lon, lat;
		while ((line = reader.readLine()) != null) {
			String[] token = line.split(",");
			lon = Integer.parseInt(token[0]);
			lat = Integer.parseInt(token[1]);
			GridCell temp = new GridCell(token[2]);
			this.cells[lon][lat] = temp;
		}
		reader.close();
	}

	public static void main(String[] args) throws FileNotFoundException,
			IOException, ParseException {
		for(queryLevel q : queryLevel.values()){
//			if(q.equals(queryLevel.Day) || q.equals(queryLevel.Week))
//				continue;
			System.out.println("************"+q.toString()+"****************");
			Grid grid = new Grid("2013-01-01", "2014-10-30",q);
			System.out.println("Building Grid for "+q.toString());
			grid.BuildGrid();
//			System.out.println("Writing Grid to Disk");
//			grid.writeGridToDisk();
//			grid.readGridFromDisk();
//			System.out.println("Writing WKT to Disk");
////			grid.writeGridToKWT();
		}
		
//		for (int i = 0; i < grid.LonDomain; i++) {
//			for (int j = 0; j < grid.LatDomain; j++) {
//				System.out.println("*************************");
//				double avg = grid.cells[i][j].getAverage();
//				double deviation = grid.cells[i][j].getStandardDeviation();
//				double error = grid.cells[i][j].getStandardError();
//				System.out.println(grid.cells[i][j].getMbr().toString());
//				System.out.println("Average: " + avg+" Deviation: "
//						+ deviation+ " Error = "+error+" %");
//				grid.cells[i][j].getMbr().toWKT();
//				break;
//
//			}
//			break;
//		}
		System.out.println("Program done");
	}

}
