/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.collections;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author turtle
 */
public class Week {
    private Date start;
    private Date end;
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public Week() {
    }

    public Week(Date start, Date end) {
        this.start = start;
        this.end = end;
    }
    
    public String getStart() {
        return dateFormat.format(this.start);
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public String getEnd() {
        return dateFormat.format(this.end);
    }

    public void setEnd(Date end) {
        this.end = end;
    }
    
    /**
     * This method parse the expression 2013-01-15&2014-10-12 To week object
     * @param args
     * @return
     * @throws ParseException 
     */
    public Week parseToWeek(String args) throws ParseException{
        String[] temp = args.split(",");
        String[] range = temp[0].split("&");
        return new Week(dateFormat.parse(range[0]),
                    dateFormat.parse(range[0]));
    }

   /**
    * This method check whether the day in a week or not
    * @param day
    * @return 
    */
    public boolean isDayIntheWeek(Date day) {
        if ((day.compareTo(start) >= 0)
                && (day.compareTo(end) <= 0)) {
            return true;
        }
        return false;
    }
    
    @Override
    public String toString() {
    	return dateFormat.format(this.start)+"&"+dateFormat.format(this.end);
    }
    
    
    
}
