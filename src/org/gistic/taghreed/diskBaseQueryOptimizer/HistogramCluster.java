package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.gistic.taghreed.collections.Partition;

public class HistogramCluster {
	List<Partition> histogram;

	public HistogramCluster(String path) throws IOException {
		File master;
		this.histogram = new ArrayList<Partition>();
		// check the master files with the index used at the backend
		master = new File(path + "/_master.quadtree");
		if (!master.exists()) {
			master = new File(path + "/_master.str");
			if (!master.exists()) {
				master = new File(path + "/_master.str+");
				if (!master.exists()) {
					master = new File(path + "/_master.grid");
				}
			}
		}

		BufferedReader reader = new BufferedReader(new FileReader(master));
		String line = null;
		while ((line = reader.readLine()) != null) {
			Partition part = new Partition(line, path, "");
			this.histogram.add(part);
		}
		reader.close();
	}

}
