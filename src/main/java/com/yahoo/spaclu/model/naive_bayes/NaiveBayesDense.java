/*
´ * Licensed to the Apache Software Foundation (ASF) under one or more
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
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.base.Preconditions;
import com.yahoo.spaclu.data.Utility;

/**
 * Naive Bayes
 */
public class NaiveBayesDense {
	final static Logger sLogger = LoggerFactory
			.getLogger(NaiveBayesDense.class);

	static class ExpectationStepTrainAggregate implements
			PairFlatMapFunction<Iterator<int[]>, Long, double[]> {
		protected final DoubleMatrix[] modelParameters;
		protected final int[] V;
		protected final int numberOfSamples;

		ExpectationStepTrainAggregate(DoubleMatrix[] modelParameters, int[] V,
				int numberOfSamples) {
			this.V = V;
			this.modelParameters = modelParameters;
			this.numberOfSamples = numberOfSamples;

			Preconditions
					.checkArgument(this.modelParameters.length == V.length + 1);
			for (int d = 0; d < V.length; d++) {
				Preconditions.checkArgument(this.modelParameters[d]
						.getColumns() == V[d]);
			}

			Preconditions.checkArgument(this.modelParameters[V.length]
					.getColumns() == 1);
		}

		public Iterable<Tuple2<Long, double[]>> call(
				Iterator<int[]> featureValuesIter) {
			Hashtable<Long, double[]> resultHashTable = new Hashtable<Long, double[]>();
			int[] featureVector;

			DoubleMatrix clusterProbability;

			while (featureValuesIter.hasNext()) {
				featureVector = featureValuesIter.next();
				clusterProbability = modelParameters[V.length].dup();

				for (int d = 0; d < V.length; d++) {
					int v = featureVector[d];

					if (v < 0) {
						continue;
					}

					if (v >= this.V[d]) {
						throw new IllegalArgumentException("unexpected value "
								+ d + " of feature " + v + "...");
					}

					clusterProbability.muliColumnVector(this.modelParameters[d]
							.getColumn(v));
				}

				clusterProbability.divi(clusterProbability.sum());

				double[] clusterProbabilityDistribution = clusterProbability.data;

				for (int d = 0; d < V.length; d++) {
					int v = featureVector[d];

					if (v <= -1) {
						continue;
					}

					if (v >= this.V[d]) {
						throw new IllegalArgumentException("unexpected value "
								+ d + " of feature " + v + "...");
					}

					long keyTriples = composeKey(d, v, V.length);
					if (!resultHashTable.contains(keyTriples)) {
						resultHashTable.put(keyTriples,
								clusterProbabilityDistribution);
					} else {
						for (int k = 0; k < clusterProbabilityDistribution.length; k++) {
							resultHashTable.get(keyTriples)[k] += clusterProbabilityDistribution[k];
						}
					}
				}
			}

			LinkedList<Tuple2<Long, double[]>> resultList = new LinkedList<Tuple2<Long, double[]>>();
			Iterator<Long> keyTripleIters = resultHashTable.keySet().iterator();
			while (keyTripleIters.hasNext()) {
				long keyTriples = keyTripleIters.next();
				resultList.add(new Tuple2<Long, double[]>(keyTriples,
						resultHashTable.get(keyTriples)));
			}

			return resultList;
		}
	}

	static class ExpectationStepTrain implements
			PairFlatMapFunction<int[], Long, double[]> {
		protected final DoubleMatrix[] modelParameters;
		protected final int[] featureValueDimension;
		protected final int numberOfSamples;
		protected final int numberOfFeatures;

		ExpectationStepTrain(DoubleMatrix[] modelParameters,
				int[] featureValueDimension) {
			this(modelParameters, featureValueDimension, 0);
		}

		ExpectationStepTrain(DoubleMatrix[] modelParameters,
				int[] featureValueDimension, int numberOfSamples) {
			this.featureValueDimension = featureValueDimension;
			this.modelParameters = modelParameters;
			this.numberOfSamples = numberOfSamples;
			this.numberOfFeatures = featureValueDimension.length;

			Preconditions
					.checkArgument(this.modelParameters.length == featureValueDimension.length + 1);
			for (int d = 0; d < featureValueDimension.length; d++) {
				Preconditions.checkArgument(this.modelParameters[d]
						.getColumns() == featureValueDimension[d]);
			}

			Preconditions
					.checkArgument(this.modelParameters[featureValueDimension.length]
							.getColumns() == 1);
		}

		public Iterable<Tuple2<Long, double[]>> call(int[] featureVector) {
			List<Tuple2<Long, double[]>> resultList = new LinkedList<Tuple2<Long, double[]>>();

			DoubleMatrix clusterProbability = modelParameters[featureValueDimension.length]
					.dup();

			for (int d = 0; d < featureValueDimension.length; d++) {
				int v = featureVector[d];

				if (v < 0) {
					continue;
				}

				if (v >= this.featureValueDimension[d]) {
					throw new IllegalArgumentException("unexpected value " + v
							+ " of feature " + d + " in data vector "
							+ Arrays.toString(featureVector) + "...");
				}

				clusterProbability.muliColumnVector(this.modelParameters[d]
						.getColumn(v));
			}

			clusterProbability.divi(clusterProbability.sum());
			double[] clusterProbabilityDistribution = clusterProbability.data;

			// TODO: add in samples
			for (int d = 0; d < featureValueDimension.length; d++) {
				int v = featureVector[d];

				if (v < 0) {
					continue;
				}

				if (v >= this.featureValueDimension[d]) {
					throw new IllegalArgumentException("unexpected value " + v
							+ " of feature " + d + "...");
				}

				resultList.add(new Tuple2<Long, double[]>(composeKey(d, v,
						numberOfFeatures), clusterProbabilityDistribution));
			}

			return resultList;
		}
	}

	static class MaximizationAggregationStep implements
			Function2<double[], double[], double[]> {

		@Override
		public double[] call(double[] a, double[] b) {
			Preconditions.checkArgument(a.length == b.length);
			double[] result = new double[a.length];
			for (int j = 0; j < result.length; j++) {
				result[j] = a[j] + b[j];
			}
			return result;
		}
	}

	public static DoubleMatrix[] trainModel(
			NaiveBayesOptions optionsNaiveBayes, JavaRDD<int[]> points,
			int[] featureValues) throws IOException {
		String outputPathString = optionsNaiveBayes.getOutputPath();
		int numberOfClusters = optionsNaiveBayes.getNumberOfClusters();
		int numberOfFeatures = optionsNaiveBayes.getNumberOfFeatures();
		int numberOfIterations = optionsNaiveBayes.getNumberOfIterations();
		int numberOfPartitions = optionsNaiveBayes.getNumberOfPartitions();
		int numberOfSamples = optionsNaiveBayes.getNumberOfSamples();

		double alpha = optionsNaiveBayes.getAlpha();
		if (alpha <= 0) {
			alpha = 1.0 / numberOfClusters;
		}

		double[] beta = NaiveBayesDriver.computeBeta(featureValues);

		DoubleMatrix[] modelParameter = NaiveBayesDriver.randomModel(
				featureValues, numberOfClusters);
		Tuple2<Long, double[]> sufficientStatisticsTuple;
		long sufficientStatisticsKey;
		double[] sufficientStatisticsValue;

		for (int i = 0; i < numberOfIterations; i++) {
			sLogger.info("On training iteration " + (i + 1));

			JavaPairRDD<Long, double[]> finalResults;

			if (true) {
				finalResults = points.mapPartitionsToPair(
						new ExpectationStepTrainAggregate(modelParameter,
								featureValues, numberOfSamples)).foldByKey(
						new double[numberOfClusters], numberOfPartitions,
						new MaximizationAggregationStep());
				finalResults.persist(StorageLevel.DISK_ONLY());
				sLogger.info("MapReduce phase for iteration " + (i + 1)
						+ " completed");
			} else {
				JavaPairRDD<Long, double[]> intermediateResults = points
						.mapPartitionsToPair(new ExpectationStepTrainAggregate(
								modelParameter, featureValues, numberOfSamples));
				/*
				 * JavaPairRDD<Long, double[]> intermediateResults = points
				 * .flatMapToPair(new ExpectationStepTrain(modelParameter,
				 * featureValues, numberOfSamples));
				 */
				intermediateResults.persist(StorageLevel.DISK_ONLY());
				sLogger.info("Map phase for iteration " + (i + 1)
						+ " completed");

				/*
				 * finalResults = intermediateResults .reduceByKey(new
				 * MaximizationStep(), numberOfPartitions);
				 */
				finalResults = intermediateResults.foldByKey(
						new double[numberOfClusters], numberOfPartitions,
						new MaximizationAggregationStep());
				finalResults.persist(StorageLevel.DISK_ONLY());
				sLogger.info("Reduce phase for iteration " + (i + 1)
						+ " completed");

				JavaPairRDD<Integer, Entry<Integer, double[]>> intermediateResults2 = finalResults
						.mapToPair(new MaximizationStepRekey(numberOfFeatures));
				finalResults.persist(StorageLevel.DISK_ONLY());
				sLogger.info("Rekey phase for iteration " + (i + 1)
						+ " completed");

				intermediateResults.unpersist();
			}

			modelParameter = null;
			// Utility.garbageCollection("Stage: after map phrase");

			DoubleMatrix[] newModelParameter = NaiveBayesDriver.uniformModel(
					alpha, beta, featureValues, numberOfClusters);

			// finalResults.saveAsTextFile(outputPathString + "final-" + (i +
			// 1));
			// sLogger.info("Save final results for iteration " + (i + 1)
			// + " to files");

			Iterator<Tuple2<Long, double[]>> iterDataPoint = finalResults
					.collect().iterator();
			sLogger.info("Constructing model parameters " + (i + 1));

			finalResults.unpersist();

			while (iterDataPoint.hasNext()) {
				sufficientStatisticsTuple = iterDataPoint.next();

				sufficientStatisticsKey = sufficientStatisticsTuple._1;
				sufficientStatisticsValue = sufficientStatisticsTuple._2;

				/*
				 * double valueSum = 0; for (double value :
				 * sufficientStatisticsValue) { valueSum += value; } for (int j
				 * = 0; j < sufficientStatisticsValue.length; j++) {
				 * sufficientStatisticsValue[j] /= valueSum; }
				 */

				int d = getFeature(sufficientStatisticsKey, numberOfFeatures);
				int v = getValue(sufficientStatisticsKey, numberOfFeatures);

				Preconditions.checkArgument(v >= 0 && v < featureValues[d],
						"unexpected value " + d + " of feature " + v + "...");

				for (int k = 0; k < numberOfClusters; k++) {
					newModelParameter[d].put(k, v,
							newModelParameter[d].get(k, v)
									+ sufficientStatisticsValue[k]);
				}

				for (int k = 0; k < numberOfClusters; k++) {
					newModelParameter[numberOfFeatures].put(k, 0,
							newModelParameter[numberOfFeatures].get(k, 0)
									+ sufficientStatisticsValue[k]);
				}
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
			sLogger.info("Update model parameter to: "
					+ Arrays.toString(modelParameter));
		}

		return modelParameter;
	}

	public static long composeKey(int d, int v, int numberOfFeatures) {
		long keyTriples = d;
		keyTriples += numberOfFeatures * v;

		return keyTriples;
	}

	public static int getFeature(long composedKey, int numberOfFeatures) {
		return (int) (composedKey % numberOfFeatures);
	}

	public static int getValue(long composedKey, int numberOfFeatures) {
		return (int) (composedKey / numberOfFeatures);
	}

	static class MaximizationStepRekey
			implements
			PairFunction<Tuple2<Long, double[]>, Integer, Entry<Integer, double[]>> {
		protected final int numberOfFeatures;

		MaximizationStepRekey(int numberOfFeatures) {
			this.numberOfFeatures = numberOfFeatures;
		}

		public Tuple2<Integer, Entry<Integer, double[]>> call(
				Tuple2<Long, double[]> sufficientStatisticsTuple) {
			Long sufficientStatisticsKey = sufficientStatisticsTuple._1;
			double[] sufficientStatisticsValue = sufficientStatisticsTuple._2;

			int d = getFeature(sufficientStatisticsKey, numberOfFeatures);
			int v = getValue(sufficientStatisticsKey, numberOfFeatures);

			return new Tuple2<Integer, Entry<Integer, double[]>>(d,
					new SimpleEntry<Integer, double[]>(v,
							sufficientStatisticsValue));
		}
	}

	static class MaximizationStepAggregate implements
			Function2<double[], double[], double[]> {
		protected final int numberOfFeatures;
		protected final int[] featureValueDimension;

		MaximizationStepAggregate(int[] featureValueDimension) {
			this.featureValueDimension = featureValueDimension;
			this.numberOfFeatures = featureValueDimension.length;
		}

		@Override
		public double[] call(double[] a, double[] b) {
			Preconditions.checkArgument(a.length == b.length);
			double[] result = new double[a.length];
			for (int j = 0; j < result.length; j++) {
				result[j] = a[j] + b[j];
			}
			return result;
		}
	}
}