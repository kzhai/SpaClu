/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.spaclu.model.naive_bayes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.base.Preconditions;
import com.yahoo.spaclu.data.Utility;

/**
 * Naive Bayes
 */
public class NaiveBayesSparse {
	final static Logger sLogger = LoggerFactory
			.getLogger(NaiveBayesSparse.class);

	static class ExpectationStepTrain implements
			PairFlatMapFunction<Iterator<int[]>, Long, Double> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5414018995361286732L;

		protected final DoubleMatrix[] modelParameters;
		protected final int[] V;
		protected final int numberOfSamples;

		protected final int numberOfFeatures;
		protected final int numberOfValues;

		// protected final int numberOfClusters;

		ExpectationStepTrain(DoubleMatrix[] modelParameters, int[] V,
				int numberOfSamples) {
			Preconditions.checkArgument(modelParameters.length == V.length + 1);
			int maxFeatureValues = 0;
			for (int d = 0; d < V.length; d++) {
				if (maxFeatureValues < V[d]) {
					maxFeatureValues = V[d];
				}

				Preconditions
						.checkArgument(modelParameters[d].getColumns() == V[d]);
			}

			Preconditions
					.checkArgument(modelParameters[V.length].getColumns() == 1);

			Preconditions.checkArgument(numberOfSamples > 0);

			this.V = V;
			this.modelParameters = modelParameters;
			this.numberOfSamples = numberOfSamples;
			this.numberOfFeatures = V.length;
			this.numberOfValues = maxFeatureValues;
		}

		public Iterable<Tuple2<Long, Double>> call(
				Iterator<int[]> featureValuesIter) {
			Hashtable<Long, Double> resultHashTable = new Hashtable<Long, Double>();
			// int[] keyTriples;
			long keyTriples;
			int[] featureValues;

			DoubleMatrix clusterProbability;

			while (featureValuesIter.hasNext()) {
				featureValues = featureValuesIter.next();
				clusterProbability = modelParameters[V.length].dup();

				for (int d = 0; d < V.length; d++) {
					int featureValue = featureValues[d];

					if (featureValue <= -1) {
						continue;
					}

					if (featureValue >= this.V[d]) {
						throw new IllegalArgumentException("unexpected value "
								+ d + " of feature " + featureValue + "...");
					}

					clusterProbability.muliColumnVector(this.modelParameters[d]
							.getColumn(featureValue));
				}

				clusterProbability.divi(clusterProbability.sum());

				int k = clusterProbability.argmax();

				for (int d = 0; d < V.length; d++) {
					int featureValue = featureValues[d];

					if (featureValue <= -1) {
						continue;
					}

					if (featureValue >= this.V[d]) {
						throw new IllegalArgumentException("unexpected value "
								+ d + " of feature " + featureValue + "...");
					}

					keyTriples = composeKey(d, featureValue, k,
							numberOfFeatures, numberOfValues);
					if (!resultHashTable.contains(keyTriples)) {
						resultHashTable.put(keyTriples, 1.0);
					} else {
						resultHashTable.put(keyTriples,
								resultHashTable.get(keyTriples) + 1.0);
					}
				}
			}

			List<Tuple2<Long, Double>> resultList = new LinkedList<Tuple2<Long, Double>>();
			Iterator<Long> keyTripleIters = resultHashTable.keySet().iterator();
			while (keyTripleIters.hasNext()) {
				keyTriples = keyTripleIters.next();
				resultList.add(new Tuple2<Long, Double>(keyTriples,
						resultHashTable.get(keyTriples)));
			}

			return resultList;
		}
	}

	static class MaximizationStep implements Function2<Double, Double, Double> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6772589261072218849L;

		public Double call(Double a, Double b) {
			return a + b;
		}
	}

	public static DoubleMatrix[] trainModel(
			NaiveBayesOptions optionsNaiveBayes, JavaRDD<int[]> points,
			int[] featureValues) throws IOException {
		int numberOfClusters = optionsNaiveBayes.getNumberOfClusters();
		int numberOfFeatures = optionsNaiveBayes.getNumberOfFeatures();
		int numberOfIterations = optionsNaiveBayes.getNumberOfIterations();
		int numberOfSamples = optionsNaiveBayes.getNumberOfSamples();

		double alpha = optionsNaiveBayes.getAlpha();
		if (alpha <= 0) {
			alpha = 1.0 / numberOfClusters;
		}

		double[] beta = NaiveBayesDriver.computeBeta(featureValues);
		int numberOfValues = NaiveBayesDriver
				.getLargestFeatureValues(featureValues);

		DoubleMatrix[] modelParameter = NaiveBayesDriver.randomModel(
				featureValues, numberOfClusters);
		Tuple2<Long, Double> sufficientStatisticsTuple;
		long sufficientStatisticsKey;
		double sufficientStatisticsValue;

		for (int i = 0; i < numberOfIterations; i++) {
			sLogger.info("On training iteration " + (i + 1));

			JavaPairRDD<Long, Double> intermediateResults = points
					.mapPartitionsToPair(new ExpectationStepTrain(
							modelParameter, featureValues, numberOfSamples));
			sLogger.info("Map phrase for iteration " + (i + 1) + " completed");

			modelParameter = null;
			Utility.garbageCollection("Stage: after map phrase");

			DoubleMatrix[] newModelParameter = NaiveBayesDriver.uniformModel(
					alpha, beta, featureValues, numberOfClusters);

			JavaPairRDD<Long, Double> finalResults = intermediateResults
					.reduceByKey(new MaximizationStep());
			sLogger.info("Reduce phrase for iteration " + (i + 1)
					+ " completed");

			Iterator<Tuple2<Long, Double>> iterDataPoint = finalResults
					.collect().iterator();
			sLogger.info("Constructing model parameters " + (i + 1));

			while (iterDataPoint.hasNext()) {
				sufficientStatisticsTuple = iterDataPoint.next();

				sufficientStatisticsKey = sufficientStatisticsTuple._1;
				sufficientStatisticsValue = sufficientStatisticsTuple._2;

				int d = getFeature(sufficientStatisticsKey, numberOfFeatures,
						numberOfValues);
				int v = getValue(sufficientStatisticsKey, numberOfFeatures,
						numberOfValues);
				int k = getCluster(sufficientStatisticsKey, numberOfFeatures,
						numberOfValues);

				Preconditions.checkArgument(v >= 0 && v < featureValues[d],
						"unexpected value " + d + " of feature " + v + "...");

				newModelParameter[d].put(k, v, newModelParameter[d].get(k, v)
						+ sufficientStatisticsValue);

				newModelParameter[numberOfFeatures].put(k, 0,
						newModelParameter[numberOfFeatures].get(k, 0)
								+ sufficientStatisticsValue);
			}

			Utility.garbageCollection("Stage: after reduce phrase");

			for (int d = 0; d < numberOfFeatures; d++) {
				newModelParameter[d].diviColumnVector(newModelParameter[d]
						.rowSums());
			}
			newModelParameter[numberOfFeatures]
					.divi(newModelParameter[numberOfFeatures].sum());

			modelParameter = newModelParameter;

			// sLogger.info("Successfully update model parameters " + (i + 1));
			sLogger.info("Update model parameter to:\n"
					+ Arrays.toString(modelParameter));
		}

		return modelParameter;
	}

	public static long composeKey(int d, int v, int k, int numberOfFeatures,
			int numberOfValues) {
		long keyTriples = d;
		keyTriples += numberOfFeatures * v;
		keyTriples += numberOfValues * numberOfFeatures * k;

		return keyTriples;
	}

	public static int getFeature(long composedKey, int numberOfFeatures,
			int numberOfValues) {
		return (int) (composedKey % (numberOfValues * numberOfFeatures) % numberOfFeatures);
	}

	public static int getValue(long composedKey, int numberOfFeatures,
			int numberOfValues) {
		return (int) (composedKey % (numberOfValues * numberOfFeatures) / numberOfFeatures);
	}

	public static int getCluster(long composedKey, int numberOfFeatures,
			int numberOfValues) {
		return (int) (composedKey / (numberOfValues * numberOfFeatures));
	}
}