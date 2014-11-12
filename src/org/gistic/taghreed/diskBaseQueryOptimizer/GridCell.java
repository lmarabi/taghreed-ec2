package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.util.HashMap;

import org.gistic.taghreed.basicgeom.MBR;

public class GridCell {
	MBR mbr; 
	HashMap<String, Long> daysCardinality = new HashMap<String, Long>();
	
	public GridCell(MBR mbr) {
		this.mbr = mbr;
	}
	
	public void add(String day,Long i){
		this.daysCardinality.put(day, i);
	}

}
