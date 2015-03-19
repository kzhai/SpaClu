package com.yahoo.spaclu.data.structure;

import com.yahoo.spaclu.model.Settings;

public class FeatureIntegerVectorClusterDoubleVector extends
		FeatureIntegerVector {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4035242356259571667L;
	private double[] clusterProbability;

	public FeatureIntegerVectorClusterDoubleVector(int[] featureValues) {
		super(featureValues);
		this.clusterProbability = null;
	}

	/**
	 * @param featureValues
	 * @param clusterProbability
	 */
	public FeatureIntegerVectorClusterDoubleVector(int[] featureValues,
			double[] clusterProbability) {
		super(featureValues);
		this.clusterProbability = clusterProbability;
	}

	public void setClusterProbability(double[] clusterProbability) {
		this.clusterProbability = clusterProbability;
	}

	public double[] getClusterProbability() {
		return this.clusterProbability;
	}

	public double getClusterProbability(int clusterIndex) {
		return this.clusterProbability[clusterIndex];
	}

	public String toString() {
		/*
		 * StringBuilder stringBuilder = new StringBuilder(); for (int i :
		 * this.featureValues) { stringBuilder.append(i);
		 * stringBuilder.append(Settings.SPACE); }
		 * stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		 */
		StringBuilder stringBuilder = new StringBuilder(super.toString());
		stringBuilder.append(Settings.TAB);
		for (double i : this.clusterProbability) {
			stringBuilder.append(i);
			stringBuilder.append(Settings.SPACE);
		}
		stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		return stringBuilder.toString();
	}
}