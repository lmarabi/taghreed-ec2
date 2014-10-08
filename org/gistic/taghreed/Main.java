package org.gistic.taghreed;

import org.eclipse.jetty.server.Server;
import org.gistic.taghreed.crawler.Crawler;
import org.gistic.taghreed.flush.TimedFlush;
import org.gistic.taghreed.webservice.WebService;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Main {

    private final static long fONCE_PER_DAY = 1000*60*60*24;

    private final static int fONE_DAY = 1;
    private final static int fFOUR_AM = 12;
    private final static int fZERO_MINUTES = 0;

    public static void main(String[] args) throws Exception {

        //loadConfigFile();

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimedFlush() , getTomorrowMorning1am() , fONCE_PER_DAY);
        System.out.println("Periodic flushing service started");
        Crawler crawler = new Crawler();
        Thread crawlerThread = new Thread(crawler);
        crawlerThread.start();
        Server server = new Server(8085);
        server.setHandler(new WebService());
        server.start();
        server.join();

        System.out.println("Server Started");

    }

    

    private static Date getTomorrowMorning1am(){
        Calendar tomorrow = new GregorianCalendar();
        tomorrow.add(Calendar.DATE, fONE_DAY);
        Calendar result = new GregorianCalendar(
                tomorrow.get(Calendar.YEAR),
                tomorrow.get(Calendar.MONTH),
                tomorrow.get(Calendar.DATE),
                fFOUR_AM,
                fZERO_MINUTES
        );
        return result.getTime();
    }
}
