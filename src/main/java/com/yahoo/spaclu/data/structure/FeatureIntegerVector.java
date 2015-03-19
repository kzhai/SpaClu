package com.yahoo.spaclu.data.structure;

import java.io.Serializable;

import com.yahoo.spaclu.model.Settings;

public class FeatureIntegerVector implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3736599956323127833L;
	protected final int[] featureValues;
	

	public FeatureIntegerVector(int[] featureValues) {
		this.featureValues = featureValues;
	}

	public int getFeatureValue(int featureIndex) {
		return this.featureValues[featureIndex];
	}

	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		for (int i : this.featureValues) {
			stringBuilder.append(i);
			stringBuilder.append(Settings.SPACE);
		}
		stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		return stringBuilder.toString();
	}
}