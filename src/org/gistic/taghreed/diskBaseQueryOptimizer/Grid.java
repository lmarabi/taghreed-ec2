package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;

import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
/**
 * This grid preprocessed to create the final grids for 
 * Day, week , Months 
 * @author louai
 *
 */
public class Grid {
	String startDay;
	String endDay;
	int LonDomain;
	int LatDomain;
	GridCell[][] cells;
	
	/**
	 * @param StartDay, EndDay
	 * Dayformat yyyy-MM-DD
	 */
	public Grid(String startDay,String endDay){
		this.startDay = startDay;
		this.endDay = endDay;
		this.LonDomain = 360;
		this.LatDomain = 180;
		this.cells = new GridCell[LonDomain][LatDomain];
	}
	
	public void BuildGrid() throws FileNotFoundException, IOException, ParseException{
		for (int lon = 0; lon < LonDomain; lon++) {
			for(int lat=0; lat < LatDomain; lat++) {
				Point min = new Point((lat-90), (lon-180));
				Point max = new Point((lat-89), (lon-179));
				MBR mbr = new MBR(max, min);
				cells[lon][lat] = new GridCell(mbr);
				System.out.println(mbr.toString());
						ServerRequest req = new ServerRequest(1);
		        req.setMBR(String.valueOf(max.getLat()), String.valueOf(max.getLon()), 
		        		String.valueOf(min.getLat()), String.valueOf(min.getLon()));
		        req.setStartDate(startDay);
		        req.setEndDate(endDay);
		        long cardinality  = req.getMasterRtreeDays();
		        System.out.println(cardinality);
				
			}
		}

	}
	
	
	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException{
		Grid grid = new Grid("2013-01-01", "2014-10-30");
		grid.BuildGrid();
	}
	

}
