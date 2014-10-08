package com.badrit.adaptiveclassifier.models;


	public class ExpansionTerm implements Comparable<ExpansionTerm> {
		public String term;
		public double tf_idf;

		public ExpansionTerm(String term, double tf_idf) {
			this.term = term;
			this.tf_idf = tf_idf;
		} 

		public String toString() {
			return "T: " + term + " tf_idf: " + tf_idf;
		}

		@Override
		public int compareTo(ExpansionTerm o) {
			return Double.compare(o.tf_idf, tf_idf);
		}

	}