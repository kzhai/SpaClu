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
import java.util.Iterator;
import java.util.LinkedList;
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
import org.jblas.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.base.Preconditions;
import com.yahoo.spaclu.data.Utility;
import com.yahoo.spaclu.data.structure.FeatureIntegerVectorClusterDoubleVector;
import com.yahoo.spaclu.data.structure.FeatureIntegerVectorClusterIntegerIndicator;
import com.yahoo.spaclu.model.Settings;

/**
 * Naive Bayes
 */
public class NaiveBayesDriver {
	final static Logger sLogger = LoggerFactory
			.getLogger(NaiveBayesDriver.class);

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

	static class FeatureValueTupleMapper implements
			PairFlatMapFunction<int[], Entry<Integer, Integer>, Integer> {
		LinkedList<Tuple2<Entry<Integer, Integer>, Integer>> linkedList;
		protected final int numberOfFeatures;

		FeatureValueTupleMapper(int numberOfFeatures) {
			this.numberOfFeatures = numberOfFeatures;
		}

		@Override
		public Iterable<Tuple2<Entry<Integer, Integer>, Integer>> call(
				int[] dataPoint) {
			linkedList = new LinkedList<Tuple2<Entry<Integer, Integer>, Integer>>();

			for (int d = 0; d < numberOfFeatures; d++) {
				int v = dataPoint[d];
				if (v < 0) {
					continue;
				}
				linkedList.add(new Tuple2<Entry<Integer, Integer>, Integer>(
						new SimpleEntry<Integer, Integer>(d, v), 1));
			}

			return linkedList;
		}
	}

	static class FeatureValueReducer implements
			Function2<Integer, Integer, Integer> {
		@Override
		public Integer call(Integer i1, Integer i2) {
			return i1 + i2;
		}
	}

	/**
	 * @deprecated
	 * @param points
	 * @param numberOfFeatures
	 * @return
	 * @throws IOException
	 */
	public static int[] collectFeatureValuesApprox(JavaRDD<int[]> points,
			int numberOfFeatures) throws IOException {
		int[] V = new int[numberOfFeatures];

		JavaRDD<Tuple2<Integer, Object>> featureValuesCount = points
				.flatMapToPair(new FeatureValueMapper(numberOfFeatures))
				.countApproxDistinctByKey(1e-6);

		Iterator<Tuple2<Integer, Object>> iter = featureValuesCount.collect()
				.iterator();
		while (iter.hasNext()) {
			Tuple2<Integer, Object> temp = iter.next();
			V[temp._1] = ((Long) temp._2).intValue();
		}

		sLogger.info("Approximate number of values for each feature: "
				+ Arrays.toString(V));

		return V;
	}

	public static int[] collectFeatureValues(JavaRDD<int[]> points,
			int numberOfFeatures) throws IOException {
		int[] V = new int[numberOfFeatures];

		JavaPairRDD<Entry<Integer, Integer>, Integer> featureValuesCount = points
				.flatMapToPair(new FeatureValueTupleMapper(numberOfFeatures))
				.reduceByKey(new FeatureValueReducer());

		Iterator<Tuple2<Entry<Integer, Integer>, Integer>> iter = featureValuesCount
				.collect().iterator();
		while (iter.hasNext()) {
			Entry<Integer, Integer> temp = iter.next()._1;
			V[temp.getKey()] += 1;
		}

		sLogger.info("Number of values for each feature: " + Arrays.toString(V));

		return V;
	}

	static class ExpectationStepTestSparse implements
			Function<int[], FeatureIntegerVectorClusterIntegerIndicator> {
		protected final DoubleMatrix[] modelParameters;
		protected final int[] V;

		ExpectationStepTestSparse(DoubleMatrix[] modelParameters, int[] V) {
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
		public FeatureIntegerVectorClusterIntegerIndicator call(int[] p) {
			FeatureIntegerVectorClusterIntegerIndicator dataPoint = new FeatureIntegerVectorClusterIntegerIndicator(
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

			double randomDouble = Random.nextDouble();
			for (int i = 0; i < clusterProbability.data.length; i++) {
				randomDouble -= clusterProbability.data[i];
				if (randomDouble <= 0) {
					dataPoint.setClusterAssignment(i);
					return dataPoint;
				}
			}

			dataPoint.setClusterAssignment(-1);
			return dataPoint;
		}
	}

	static class ExpectationStepTestDense implements
			Function<int[], FeatureIntegerVectorClusterDoubleVector> {
		protected final DoubleMatrix[] modelParameters;
		protected final int[] V;

		ExpectationStepTestDense(DoubleMatrix[] modelParameters, int[] V) {
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

	public static double[] computeBeta(int[] featureValues) {
		double[] beta = new double[featureValues.length];
		for (int d = 0; d < featureValues.length; d++) {
			beta[d] = 1.0 / featureValues[d];
		}

		sLogger.info("Beta value for each feature: " + Arrays.toString(beta));

		return beta;
	}

	public static int getLargestFeatureValues(int[] featureValues) {
		int numberOfValues = 0;
		for (int d = 0; d < featureValues.length; d++) {
			if (numberOfValues < featureValues[d]) {
				numberOfValues = featureValues[d];
			}
		}

		return numberOfValues;
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

		sLogger.info("Tool: " + NaiveBayesDriver.class.getSimpleName());
		sLogger.info(" - input path: " + inputPathString);
		sLogger.info(" - output path: " + outputPathString);
		sLogger.info(" - number of clusters: " + numberOfClusters);
		sLogger.info(" - number of features: " + numberOfFeatures);
		sLogger.info(" - number of iterations: " + numberOfIterations);
		sLogger.info(" - number of partitions: " + numberOfPartitions);
		sLogger.info(" - number of samples: " + numberOfSamples);
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

		// shuffle configs
		sparkConf = sparkConf.set("spark.default.parallelism", "320");
		sparkConf = sparkConf.set("spark.shuffle.consolidateFiles", "true");

		sparkConf = sparkConf.set("spark.shuffle.file.buffer.kb", "200");
		sparkConf = sparkConf.set("spark.reducer.maxMbInFlight", "96");

		sparkConf = sparkConf.set("spark.rdd.compress", "true");

		sparkConf = sparkConf.set("spark.shuffle.memoryFraction", "0.9");
		sparkConf = sparkConf.set("spark.storage.memoryFraction", "0");

		// we ran into a problem with the default timeout of 60 seconds
		// this is also being set in the master's spark-env.sh. Not sure if it
		// needs to be in both places
		sparkConf = sparkConf.set("spark.worker.timeout", "300");

		// akka settings
		sparkConf = sparkConf.set("spark.akka.threads", "300");
		sparkConf = sparkConf.set("spark.akka.askTimeout", "300");
		sparkConf = sparkConf.set("spark.akka.timeout", "300");
		sparkConf = sparkConf.set("spark.akka.frameSize", "512");
		sparkConf = sparkConf.set("spark.akka.batchSize", "30");

		sparkConf = sparkConf.set("spark.cores.max", "8");
		sparkConf = sparkConf.set("spark.kryoserializer.buffer.mb", "512");

		// block manager
		sparkConf = sparkConf.set(
				"spark.storage.blockManagerTimeoutIntervalMs", "180000");
		sparkConf = sparkConf.set("spark.blockManagerHeartBeatMs", "80000");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(inputPathString);

		JavaRDD<int[]> points = lines
				.map(new ParseStringToFeatureVector(numberOfFeatures))
				.repartition(1000).cache();

		int[] featureValues = collectFeatureValues(points, numberOfFeatures);

		Utility.garbageCollection("Stage: collect feature values");

		points = points.repartition(numberOfPartitions);

		DoubleMatrix[] modelParameter;
		if (numberOfSamples <= 0) {
			modelParameter = NaiveBayesDense.trainModel(optionsNaiveBayes,
					points, featureValues);
		} else {
			modelParameter = NaiveBayesSparse.trainModel(optionsNaiveBayes,
					points, featureValues);
		}

		testModel(optionsNaiveBayes, modelParameter, points, featureValues);

		sc.stop();
	}

	public static void testModel(NaiveBayesOptions optionsNaiveBayes,
			DoubleMatrix[] modelParameter, JavaRDD<int[]> points,
			int[] featureValues) {
		String outputPathString = optionsNaiveBayes.getOutputPath();

		sLogger.info("On testing inference");
		points.map(new ExpectationStepTestSparse(modelParameter, featureValues))
				.saveAsTextFile(outputPathString + "data");
	}

	final public static DoubleMatrix[] randomModel(int[] V, int numberOfClusters) {
		DoubleMatrix[] modelParameters = new DoubleMatrix[V.length + 1];
		for (int d = 0; d < V.length; d++) {
			modelParameters[d] = DoubleMatrix.rand(numberOfClusters, V[d]);
			modelParameters[d].diviColumnVector(modelParameters[d].rowSums());
		}

		modelParameters[V.length] = DoubleMatrix.rand(numberOfClusters, 1);
		modelParameters[V.length].divi(modelParameters[V.length].sum());

		return modelParameters;
	}

	final public static DoubleMatrix[] uniformModel(double alpha,
			double[] beta, int[] V, int numberOfClusters) {
		DoubleMatrix[] modelParameters = new DoubleMatrix[V.length + 1];
		for (int d = 0; d < V.length; d++) {
			modelParameters[d] = new DoubleMatrix(numberOfClusters, V[d]);
			modelParameters[d].fill(beta[d]);
			modelParameters[d].diviColumnVector(modelParameters[d].rowSums());
		}

		modelParameters[V.length] = new DoubleMatrix(numberOfClusters, 1);
		modelParameters[V.length].fill(alpha);
		modelParameters[V.length].divi(modelParameters[V.length].sum());

		return modelParameters;
	}
}