package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.util.List;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;



public class ConfidenceCoefficient {
	private static double  confidencecoefficient;
	private static double confidenceLevel;
	private static int sampleSize;
	
	
	public static double calcMeanCI(List<Integer> list,int ConfidenceLevel) {
		confidenceLevel = confidenceLevel/100;
		sampleSize = list.size();
		 SummaryStatistics stats = new SummaryStatistics();
	        for (Integer val : list) {
	            stats.addValue(val);
	        }
		
        try {
            // Create T Distribution with N-1 degrees of freedom
            TDistribution tDist = new TDistribution(stats.getN() - 1);
            // Calculate critical value
            double critVal = tDist.inverseCumulativeProbability(1.0 - (1 - confidenceLevel) / 2);
            // Calculate confidence interval
            return critVal * stats.getStandardDeviation() / Math.sqrt(stats.getN());
        } catch (MathIllegalArgumentException e) {
            return Double.NaN;
        }
    }
	


}
