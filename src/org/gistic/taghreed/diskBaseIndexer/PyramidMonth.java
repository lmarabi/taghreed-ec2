/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseIndexer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author turtle
 */
public class PyramidMonth {
    private List<PyramidWeek> weeks = new ArrayList<PyramidWeek>();
    private int year;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public PyramidMonth() {
    }

    public PyramidMonth(String date) throws ParseException {
         Calendar c = Calendar.getInstance();
        Date day = dateFormat.parse(date);
        c.setTime(day);
       this.year = c.get(Calendar.YEAR);
    }

    
    public void setWeeks(List<PyramidWeek> weeks) {
        this.weeks = weeks;
    }

    public List<PyramidWeek> getWeeks() {
        return weeks;
    }
    
    public void addWeek(PyramidWeek week){
        this.weeks.add(week);
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getYear() {
        return year;
    }
    
}
