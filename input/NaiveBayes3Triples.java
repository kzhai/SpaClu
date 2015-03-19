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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
import com.yahoo.spaclu.data.structure.Triples;
import com.yahoo.spaclu.model.Settings;

/**
 * Naive Bayes
 */
public class NaiveBayes3Triples {
	public static final int NUMBER_OF_PARTITIONS = 10;

	final static Logger sLogger = LoggerFactory
			.getLogger(NaiveBayes3Triples.class);

	static class ParseLine implements Function<String, int[]> {
		protected final int numberOfFeatures;

		ParseLine(int numberOfFeatures) {
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

	static class MaximizationStep implements Function2<Double, Double, Double> {
		public Double call(Double a, Double b) {
			return a + b;
		}
	}

	static class ExpectationStepTrain
			implements
			PairFlatMapFunction<int[], Triples<Integer, Integer, Integer>, Double> {
		protected final DoubleMatrix[] modelParameters;
		protected final int[] V;

		ExpectationStepTrain(DoubleMatrix[] modelParameters, int[] V) {
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

		public Iterable<Tuple2<Triples<Integer, Integer, Integer>, Double>> call(
				int[] featureValues) {
			List<Tuple2<Triples<Integer, Integer, Integer>, Double>> resultList = new LinkedList<Tuple2<Triples<Integer, Integer, Integer>, Double>>();

			DoubleMatrix clusterProbability = modelParameters[V.length].dup();

			// Collections.shuffle(indexList);
			// for (int d : indexList) {
			for (int d = 0; d < V.length; d++) {
				int featureValue = featureValues[d];

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
			double[] clusterProbabilityDistribution = clusterProbability.data;

			for (int d = 0; d < V.length; d++) {
				int featureValue = featureValues[d];

				if (featureValue <= -1) {
					continue;
				}

				if (featureValue >= this.V[d]) {
					throw new IllegalArgumentException("unexpected value " + d
							+ " of feature " + featureValue + "...");
				}

				for (int k = 0; k < clusterProbabilityDistribution.length; k++) {
					resultList
							.add(new Tuple2<Triples<Integer, Integer, Integer>, Double>(
									new Triples<Integer, Integer, Integer>(d,
											k, featureValue),
									clusterProbabilityDistribution[k]));
				}
			}

			return resultList;
		}
	}

	static class ExpectationStepTest implements
			Function<int[], FeatureIntegerVectorClusterDoubleVector> {
		protected final DoubleMatrix[] modelParameters;
		protected final int[] V;

		ExpectationStepTest(DoubleMatrix[] modelParameters, int[] V) {
			this.V = V;
			this.modelParameters = modelParameters;

			Preconditions
					.checkArgument(this.modelParameters.length == V.length + 1);
			for (int d = 0; d < V.length; d++) {
				// Preconditions.checkArgument(this.modelParameters[d].getRows()
				// == numberOfClusters);
				Preconditions.checkArgument(this.modelParameters[d]
						.getColumns() == V[d]);
			}

			Preconditions.checkArgument(this.modelParameters[V.length]
					.getColumns() == 1);
			// Preconditions.checkArgument(this.modelParameters[V.length].getRows()
			// == numberOfClusters);
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
		double alpha = optionsNaiveBayes.getAlpha();
		if (alpha <= 0) {
			alpha = 1.0 / numberOfClusters;
		}

		int[] V = new int[numberOfFeatures];
		double[] beta = new double[numberOfFeatures];

		sLogger.info("Tool: " + NaiveBayes3Triples.class.getSimpleName());
		sLogger.info(" - input path: " + inputPathString);
		sLogger.info(" - output path: " + outputPathString);
		sLogger.info(" - number of clusters: " + numberOfClusters);
		sLogger.info(" - number of features: " + numberOfFeatures);
		sLogger.info(" - number of iterations: " + numberOfIterations);
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
		// sparkConf.set("spark.executor.memory", "8g");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(inputPathString).repartition(
				NUMBER_OF_PARTITIONS);
		// .coalesce(1000, true);

		JavaRDD<int[]> points = lines.map(new ParseLine(numberOfFeatures))
				.repartition(NUMBER_OF_PARTITIONS).cache();
		// .coalesce(1000, true).cache();
		// .persist(StorageLevel.MEMORY_AND_DISK());

		JavaRDD<Tuple2<Integer, Object>> featureValuesCount = points
				.flatMapToPair(new FeatureValueMapper(numberOfFeatures))
				.countApproxDistinctByKey(1e-3);
		/*
		 * JavaPairRDD<Integer, Integer> featureValues = points
		 * .flatMapToPair(new FeatureValueMapper());
		 * 
		 * JavaRDD<Tuple2<Integer, Object>> featureValuesCount = featureValues
		 * .countApproxDistinctByKey(1e-3);
		 */

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
		Tuple2<Triples<Integer, Integer, Integer>, Double> sufficientStatistics;
		Triples<Integer, Integer, Integer> sufficientStatisticsKey;
		double sufficientStatisticsValue;

		for (int i = 0; i < numberOfIterations; i++) {
			sLogger.info("On iteration " + (i + 1));

			JavaPairRDD<Triples<Integer, Integer, Integer>, Double> intermediateResults = points
					.flatMapToPair(new ExpectationStepTrain(modelParameter, V))
					.repartition(NUMBER_OF_PARTITIONS).cache();

			sLogger.info("Map phrase for iteration " + (i + 1) + " completed");

			// JavaPairRDD<Entry<Integer, Integer>, double[]>
			// aggregatedResultsByFeatureValue =
			// intermediateResults.reduceByKey(new MaximizationStep());
			// JavaRDD<Tuple2<Entry<Integer, Integer>, double[]>>
			// aggregatedResults =
			// JavaPairRDD.toRDD(aggregatedResultsByFeatureValue).toJavaRDD();

			// TODO: get rid of collect
			Iterator<Tuple2<Triples<Integer, Integer, Integer>, Double>> iterDataPoint = intermediateResults
					.reduceByKey(new MaximizationStep()).collect().iterator();

			sLogger.info("Reduce phrase for iteration " + (i + 1)
					+ " completed");
			sLogger.info("Constructing model parameters " + (i + 1));

			DoubleMatrix[] newModelParameter = NaiveBayes3Long.uniformModel(alpha,
					beta, V, numberOfClusters);

			while (iterDataPoint.hasNext()) {
				sufficientStatistics = iterDataPoint.next();

				sufficientStatisticsKey = sufficientStatistics._1;
				sufficientStatisticsValue = sufficientStatistics._2;

				int d = sufficientStatisticsKey.getFirst();
				int k = sufficientStatisticsKey.getSecond();
				int featureValue = sufficientStatisticsKey.getThird();

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

			for (int d = 0; d < numberOfFeatures; d++) {
				newModelParameter[d].diviColumnVector(newModelParameter[d]
						.rowSums());
			}
			newModelParameter[numberOfFeatures]
					.divi(newModelParameter[numberOfFeatures].sum());

			modelParameter = newModelParameter;
		}

		sLogger.info("On inference");
		JavaRDD<FeatureIntegerVectorClusterDoubleVector> iterDataPoint = points
				.map(new ExpectationStepTest(modelParameter, V));
		// TODO: change it to save matrix
		Utility.printJavaRDD(iterDataPoint);

		sc.stop();
	}
}