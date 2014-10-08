package org.gistic.taghreed.indexers;

import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.gistic.taghreed.Commons;

import java.io.File;
import java.io.IOException;
import java.util.TimerTask;

/**
 * Created by saifalharthi on 5/21/14.
 */
public class FlushScheduler extends TimerTask {

    Directory ramDirectory;

    public FlushScheduler(Directory ramDirectory) {
        this.ramDirectory = ramDirectory;
    }

    @Override
    public void run() {

        try {
            Commons commons = new Commons();
            Directory dir = FSDirectory.open(new File(commons.getTweetFlushDir()));
            IndexWriter writer = new IndexWriter(dir , new IndexWriterConfig(Version.LUCENE_47 , new ArabicAnalyzer(Version.LUCENE_47)));
            writer.addIndexes(ramDirectory);
            writer.close();
            ramDirectory.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
