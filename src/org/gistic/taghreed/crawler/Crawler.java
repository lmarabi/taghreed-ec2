package org.gistic.taghreed.crawler;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.gistic.taghreed.flush.CSVConverter;
import org.gistic.taghreed.indexers.KeywordIndexer;
import org.gistic.taghreed.indexers.QuadTreeIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by saifalharthi on 5/17/14.
 */
public class Crawler implements Runnable {


    public static final Directory ramDir = new RAMDirectory();
    public static final Directory quadRamDir = new RAMDirectory();
    KeywordIndexer KWIndexer = new KeywordIndexer();
    QuadTreeIndex quadTreeIndex  = new QuadTreeIndex();
    public void crawl() throws InterruptedException, IOException, ParseException, java.text.ParseException {



        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(300000);



        Authentication auth = new OAuth1("HkExeK3KOD4UzuxLkXOTmOHV9" , "hAEZMKtoDKoz6X4iSXQA7BfInyzN0YnNXuoGaQ8BRaeZMNhr7W" , "28900074-Zdvt5pUJLNtuoIQSggha9oHwXlrruKhutjYIRMp0c" , "xakbYNnc9f9zDI0Qs0fyDQhdV99tlGW3dC8oBwXRLt51U");


        BasicClient client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .authentication(auth)
                .endpoint(new StatusesSampleEndpoint())
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();

        while (!client.isDone())
        {
            String message = queue.take();

            if(message.contains("created_at")) {
                KWIndexer.parseAndIndexTweet(message);

                quadTreeIndex.parseAndIndexTweet(message);

            }


            //System.out.println(message);

        }
    }

    @Override
    public void run() {
        try {
            System.out.println("Started Crawler");
            crawl();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
