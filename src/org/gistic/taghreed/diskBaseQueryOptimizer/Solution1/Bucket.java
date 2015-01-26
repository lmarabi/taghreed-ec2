package org.gistic.taghreed.diskBaseQueryOptimizer.Solution1;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.gistic.taghreed.basicgeom.MBR;

public class Bucket implements Comparable<Bucket>{
	private int id;
    private MBR area;
    private long cardinality;
    private double persent;
    

    public Bucket() {
    }
    
    public void setId(int id) {
		this.id = id;
	}
    
    public int getId() {
		return id;
	}

    public MBR getArea() {
        return area;
    }

    public void setArea(MBR area) {
        this.area = area;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }
    
    public void setPersent(double persent) {
		this.persent = persent;
	}
    
    public double getPersent() {
		return persent;
	}

    /**
     * Cardinality of the partition is the number of rows in this partition
     * @return 
     */
    public long getCardinality() {
        return cardinality;
    }
    
    
    public String partitionToWKT(){
		return this.id + "\tPOLYGON (("
    +this.area.getMax().getLon() + " "+ this.area.getMin().getLat() 
    +", "+ this.area.getMax().getLon() + " "+ this.area.getMax().getLat()
    +", "+ this.area.getMin().getLon() + " "+ this.area.getMax().getLat()
    +", "+ this.area.getMin().getLon() + " "+ this.area.getMin().getLat()
    +", "+ this.area.getMax().getLon() + " "+ this.area.getMin().getLat()
    + "))\t" + this.persent;
	}

	@Override
	public int compareTo(Bucket arg0) {
		return arg0.id - id ;
	}
}
