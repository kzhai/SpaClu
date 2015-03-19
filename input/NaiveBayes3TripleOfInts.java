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
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.base.Preconditions;
import com.yahoo.spaclu.data.Utility;
import com.yahoo.spaclu.data.structure.FeatureIntegerVectorClusterDoubleVector;
import com.yahoo.spaclu.data.structure.TripleOfInts;
import com.yahoo.spaclu.model.Settings;

/**
 * Naive Bayes
 */
public class NaiveBayes3TripleOfInts {
	final static int MB = 1024 * 1024;

	final static Logger sLogger = LoggerFactory
			.getLogger(NaiveBayes3TripleOfInts.class);

	static class ParseStringToFeatureVector implements Function<String, int[]> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3897484952874238367L;

		protected final int numberOfFeatures;

		ParseStringToFeatureVector(int numberOfFeatures) {
			this.numberOfFeatures = numberOfFeatures;
		}

		@Override
		public int[] call(String line) {
			String[] tok = Settings.SPACE_PATTERN.split(line);
			int[] x = new int[numberOfFeatures];
			for (int i = 0; i < numberOfFeatures; i++) {
				x[i] = Integer.parseInt(tok[i]);
			}
			return x;// new DataPoint(x);
		}
	}

	static class FeatureValueMapper implements
			PairFlatMapFunction<int[], Integer, Integer> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4733017135089682133L;

		LinkedList<Tuple2<Integer, Integer>> linkedList;
		protected final int numberOfFeatures;

		FeatureValueMapper(int numberOfFeatures) {
			this.numberOfFeatures = numberOfFeatures;
		}

		@Override
		public Iterable<Tuple2<Integer, Integer>> call(int[] dataPoint) {
			linkedList = new LinkedList<Tuple2<Integer, Integer>>();

			for (int d = 0; d < numberOfFeatures; d++) {
				int v = dataPoint[d];
				if (v < 0) {
					continue;
				}
				linkedList.add(new Tuple2<Integer, Integer>(d, v));
			}

			return linkedList;
		}
	}

	static class ExpectationStepTrain implements
			PairFlatMapFunction<Iterator<int[]>, TripleOfInts, Double> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5414018995361286732L;

		protected final DoubleMatrix[] modelParameters;
		protected final int[] V;
		protected final int numberOfSamples;

		ExpectationStepTrain(DoubleMatrix[] modelParameters, int[] V,
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

		public Iterable<Tuple2<TripleOfInts, Double>> call(
				Iterator<int[]> featureValuesIter) {
			Hashtable<TripleOfInts, Double> resultHashTable = new Hashtable<TripleOfInts, Double>();
			TripleOfInts keyTriples;
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

				if (numberOfSamples <= 0) {
					double[] clusterProbabilityDistribution = clusterProbability.data;

					for (int d = 0; d < V.length; d++) {
						int featureValue = featureValues[d];

						if (featureValue <= -1) {
							continue;
						}

						if (featureValue >= this.V[d]) {
							throw new IllegalArgumentException(
									"unexpected value " + d + " of feature "
											+ featureValue + "...");
						}

						for (int k = 0; k < clusterProbabilityDistribution.length; k++) {
							keyTriples = new TripleOfInts(d, k, featureValue);
							if (!resultHashTable.contains(keyTriples)) {
								resultHashTable.put(keyTriples,
										clusterProbabilityDistribution[k]);
							} else {
								resultHashTable
										.put(keyTriples,
												resultHashTable.get(keyTriples)
														+ clusterProbabilityDistribution[k]);
							}
						}
					}
				} else {
					int k = clusterProbability.argmax();

					for (int d = 0; d < V.length; d++) {
						int featureValue = featureValues[d];

						if (featureValue <= -1) {
							continue;
						}

						if (featureValue >= this.V[d]) {
							throw new IllegalArgumentException(
									"unexpected value " + d + " of feature "
											+ featureValue + "...");
						}

						keyTriples = new TripleOfInts(d, k, featureValue);
						if (!resultHashTable.contains(keyTriples)) {
							resultHashTable.put(keyTriples, 1.0);
						} else {
							resultHashTable.put(keyTriples,
									resultHashTable.get(keyTriples) + 1.0);
						}
					}
				}
			}

			List<Tuple2<TripleOfInts, Double>> resultList = new LinkedList<Tuple2<TripleOfInts, Double>>();
			Iterator<TripleOfInts> keyTripleIters = resultHashTable.keySet()
					.iterator();
			while (keyTripleIters.hasNext()) {
				keyTriples = keyTripleIters.next();
				resultList.add(new Tuple2<TripleOfInts, Double>(keyTriples,
						resultHashTable.get(keyTriples)));
			}

			/*
			 * Runtime.getRuntime().runFinalization();
			 * System.out.println("Memory profiler before gc():\t" +
			 * Runtime.getRuntime().totalMemory() / MB + "\t" +
			 * Runtime.getRuntime().freeMemory() / MB + "\t" +
			 * (Runtime.getRuntime().totalMemory() - Runtime
			 * .getRuntime().freeMemory()) / MB);
			 * 
			 * Runtime.getRuntime().gc();
			 * Runtime.getRuntime().runFinalization();
			 * System.out.println("Memory profiler after gc():\t" +
			 * Runtime.getRuntime().totalMemory() / MB + "\t" +
			 * Runtime.getRuntime().freeMemory() / MB + "\t" +
			 * (Runtime.getRuntime().totalMemory() - Runtime
			 * .getRuntime().freeMemory()) / MB);
			 */

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

	static class ExpectationStepTest implements
			Function<int[], FeatureIntegerVectorClusterDoubleVector> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2616038196378331862L;

		protected final DoubleMatrix[] modelParameters;
		protected final int[] V;

		ExpectationStepTest(DoubleMatrix[] modelParameters, int[] V) {
			this.V = V;
			this.modelParameters = modelParameters;

			Preconditions
					.checkArgument(this.modelParameters.length == V.length + 1);
			for (int d = 0; d < V.length; d++) {
				Preconditions.checkArgument(this.modelParameters[d]
						.getColumns() == V[d]);
			}

			Preconditions.checkArgument(this.modelParameters[V.length]
					.getColumns() == 1);
		}

		@Override
		public FeatureIntegerVectorClusterDoubleVector call(int[] p) {
			FeatureIntegerVectorClusterDoubleVector dataPoint = new FeatureIntegerVectorClusterDoubleVector(
					p);

			DoubleMatrix clusterProbability = modelParameters[V.length].dup();

			for (int d = 0; d < V.length; d++) {
				int featureValue = p[d];

				if (featureValue <= -1) {
					continue;
				}

				if (featureValue >= this.V[d]) {
					throw new IllegalArgumentException("unexpected value " + d
							+ " of feature " + featureValue + "...");
				}

				clusterProbability.muliColumnVector(this.modelParameters[d]
						.getColumn(featureValue));
			}

			clusterProbability.divi(clusterProbability.sum());

			dataPoint.setClusterProbability(clusterProbability.data);

			return dataPoint;
		}
	}

	public static void main(String[] args) throws IOException {
		NaiveBayesOptions optionsNaiveBayes = new NaiveBayesOptions(args);

		String inputPathString = optionsNaiveBayes.getInputPath();
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

		int[] V = new int[numberOfFeatures];
		double[] beta = new double[numberOfFeatures];

		outputPathString += Utility.getDateTime() + Settings.SEPERATOR;

		sLogger.info("Tool: " + NaiveBayes3TripleOfInts.class.getSimpleName());
		sLogger.info(" - input path: " + inputPathString);
		sLogger.info(" - output path: " + outputPathString);
		sLogger.info(" - number of clusters: " + numberOfClusters);
		sLogger.info(" - number of features: " + numberOfFeatures);
		sLogger.info(" - number of iterations: " + numberOfIterations);
		sLogger.info(" - number of partitions: " + numberOfPartitions);
		sLogger.info(" - number of sample: " + numberOfSamples);
		sLogger.info(" - alpha: " + alpha);

		// Create a default hadoop configuration
		Configuration conf = new Configuration();

		// Parse created config to the HDFS
		FileSystem fs = FileSystem.get(conf);

		Path outputPath = new Path(outputPathString);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		SparkConf sparkConf = new SparkConf().setAppName(optionsNaiveBayes
				.toString());

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(inputPathString);

		JavaRDD<int[]> points = lines
				.map(new ParseStringToFeatureVector(numberOfFeatures))
				.repartition(numberOfPartitions).cache();

		JavaRDD<Tuple2<Integer, Object>> featureValuesCount = points
				.flatMapToPair(new FeatureValueMapper(numberOfFeatures))
				.countApproxDistinctByKey(1e-3);

		Iterator<Tuple2<Integer, Object>> iter = featureValuesCount.collect()
				.iterator();
		while (iter.hasNext()) {
			Tuple2<Integer, Object> temp = iter.next();
			V[temp._1] = ((Long) temp._2).intValue();
		}

		for (int d = 0; d < numberOfFeatures; d++) {
			beta[d] = 1.0 / V[d];
		}

		sLogger.info("Number of value for each feature: " + Arrays.toString(V));
		sLogger.info("Beta value for each feature: " + Arrays.toString(beta));

		DoubleMatrix[] modelParameter = NaiveBayes3Long.randomModel(V,
				numberOfClusters);
		Tuple2<TripleOfInts, Double> sufficientStatisticsTuple;
		Entry<TripleOfInts, Double> sufficientStatisticsEntry;
		TripleOfInts sufficientStatisticsKey;
		double sufficientStatisticsValue;

		for (int i = 0; i < numberOfIterations; i++) {
			sLogger.info("On training iteration " + (i + 1));

			JavaPairRDD<TripleOfInts, Double> intermediateResults = points
					.mapPartitionsToPair(new ExpectationStepTrain(
							modelParameter, V, numberOfSamples));
			sLogger.info("Map phrase for iteration " + (i + 1) + " completed");

			/*
			 * intermediateResults.saveAsTextFile(outputPathString +
			 * "intermediate-"+(i+1));
			 * sLogger.info("Save intermediate results for iteration " + (i + 1)
			 * + " to files"); sLogger.info("Number of intermediate results: " +
			 * intermediateResults.count());
			 */

			DoubleMatrix[] newModelParameter = NaiveBayes3Long.uniformModel(alpha,
					beta, V, numberOfClusters);

			if (false) {
				Iterator<Entry<TripleOfInts, Double>> iterDataPoint = intermediateResults
						.reduceByKeyLocally(new MaximizationStep()).entrySet()
						.iterator();

				while (iterDataPoint.hasNext()) {
					sufficientStatisticsEntry = iterDataPoint.next();

					sufficientStatisticsKey = sufficientStatisticsEntry
							.getKey();
					sufficientStatisticsValue = sufficientStatisticsEntry
							.getValue();

					int d = sufficientStatisticsKey.getLeftElement();
					int k = sufficientStatisticsKey.getMiddleElement();
					int featureValue = sufficientStatisticsKey
							.getRightElement();

					Preconditions.checkArgument(featureValue >= 0
							&& featureValue < V[d], "unexpected value " + d
							+ " of feature " + featureValue + "...");

					newModelParameter[d].put(k, featureValue,
							newModelParameter[d].get(k, featureValue)
									+ sufficientStatisticsValue);

					newModelParameter[numberOfFeatures].put(k, 0,
							newModelParameter[numberOfFeatures].get(k, 0)
									+ sufficientStatisticsValue);
				}
			} else {
				JavaPairRDD<TripleOfInts, Double> finalResults = intermediateResults
						.reduceByKey(new MaximizationStep(),
								numberOfPartitions * 1000);
				sLogger.info("Reduce phrase for iteration " + (i + 1)
						+ " completed");

				finalResults.saveAsTextFile(outputPathString + "final-"
						+ (i + 1));
				sLogger.info("Save final results for iteration " + (i + 1)
						+ " to files");

				Runtime.getRuntime().runFinalization();
				System.out.println("Memory profiler before gc():\t"
						+ Runtime.getRuntime().totalMemory()
						/ MB
						+ "\t"
						+ Runtime.getRuntime().freeMemory()
						/ MB
						+ "\t"
						+ (Runtime.getRuntime().totalMemory() - Runtime
								.getRuntime().freeMemory()) / MB);

				Runtime.getRuntime().gc();
				Runtime.getRuntime().runFinalization();
				System.out.println("Memory profiler after gc():\t"
						+ Runtime.getRuntime().totalMemory()
						/ MB
						+ "\t"
						+ Runtime.getRuntime().freeMemory()
						/ MB
						+ "\t"
						+ (Runtime.getRuntime().totalMemory() - Runtime
								.getRuntime().freeMemory()) / MB);

				Iterator<Tuple2<TripleOfInts, Double>> iterDataPoint = finalResults
						.collect().iterator();
				sLogger.info("Constructing model parameters " + (i + 1));

				while (iterDataPoint.hasNext()) {
					sufficientStatisticsTuple = iterDataPoint.next();

					sufficientStatisticsKey = sufficientStatisticsTuple._1;
					sufficientStatisticsValue = sufficientStatisticsTuple._2;

					int d = sufficientStatisticsKey.getLeftElement();
					int k = sufficientStatisticsKey.getMiddleElement();
					int featureValue = sufficientStatisticsKey
							.getRightElement();

					Preconditions.checkArgument(featureValue >= 0
							&& featureValue < V[d], "unexpected value " + d
							+ " of feature " + featureValue + "...");

					newModelParameter[d].put(k, featureValue,
							newModelParameter[d].get(k, featureValue)
									+ sufficientStatisticsValue);

					newModelParameter[numberOfFeatures].put(k, 0,
							newModelParameter[numberOfFeatures].get(k, 0)
									+ sufficientStatisticsValue);
				}
			}

			for (int d = 0; d < numberOfFeatures; d++) {
				newModelParameter[d].diviColumnVector(newModelParameter[d]
						.rowSums());
			}
			newModelParameter[numberOfFeatures]
					.divi(newModelParameter[numberOfFeatures].sum());

			modelParameter = newModelParameter;

			sLogger.info("Update model parameter to:\n"
					+ Arrays.toString(modelParameter));
		}

		sLogger.info("On testing inference");
		points.map(new ExpectationStepTest(modelParameter, V)).saveAsTextFile(
				outputPathString + "data");

		sc.stop();
	}
}