package org.gistic.taghreed.collections;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by saifalharthi on 5/29/14.
 */
public class TweetVolumes implements Comparable<TweetVolumes> {

    public String dayName;
    public int volume;
    Date dateDayName;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public TweetVolumes(Date day, int volume) throws ParseException {  
        this.volume = volume;
        this.dateDayName = day;
        this.dayName = sdf.format(day);
    }

    public TweetVolumes(String dayName, int volume) {
        this.dayName = dayName;
        this.volume = volume;
        try {
            this.dateDayName = sdf.parse(dayName);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    @Override
    public int compareTo(TweetVolumes o) {
        return dateDayName.compareTo(o.dateDayName);
    }
}
