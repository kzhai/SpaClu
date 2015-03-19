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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.spark.SparkConf;
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
import com.yahoo.spaclu.model.Settings;

/**
 * Naive Bayes
 */
public final class NaiveBayes1 {
	final static Logger sLogger = LoggerFactory.getLogger(NaiveBayes1.class);

	static class ParseLine implements
			Function<String, FeatureIntegerVectorClusterDoubleVector> {
		protected final int numberOfFeatures;

		ParseLine(int numberOfFeatures) {
			this.numberOfFeatures = numberOfFeatures;
		}

		@Override
		public FeatureIntegerVectorClusterDoubleVector call(String line) {
			String[] tok = Settings.SPACE_PATTERN.split(line);
			int[] x = new int[numberOfFeatures];
			for (int i = 0; i < numberOfFeatures; i++) {
				x[i] = Integer.parseInt(tok[i]);
			}
			return new FeatureIntegerVectorClusterDoubleVector(x);
		}
	}

	static class FeatureValueMapper
			implements
			PairFlatMapFunction<FeatureIntegerVectorClusterDoubleVector, Integer, Integer> {
		LinkedList<Tuple2<Integer, Integer>> linkedList;
		protected final int numberOfFeatures;

		FeatureValueMapper(int numberOfFeatures) {
			this.numberOfFeatures = numberOfFeatures;
		}

		@Override
		public Iterable<Tuple2<Integer, Integer>> call(
				FeatureIntegerVectorClusterDoubleVector dataPoint) {
			linkedList = new LinkedList<Tuple2<Integer, Integer>>();

			for (int d = 0; d < numberOfFeatures; d++) {
				int v = dataPoint.getFeatureValue(d);
				if (v < 0) {
					continue;
				}
				linkedList.add(new Tuple2<Integer, Integer>(d, v));
			}

			return linkedList;
		}
	}

	static class MaximizationStep implements
			Function2<double[], double[], double[]> {
		protected final int numberOfClusters;

		MaximizationStep(int numberOfClusters) {
			this.numberOfClusters = numberOfClusters;
		}

		@Override
		public double[] call(double[] a, double[] b) {
			Preconditions.checkArgument(a.length == b.length);
			double[] result = new double[numberOfClusters];
			for (int j = 0; j < numberOfClusters; j++) {
				result[j] = a[j] + b[j];
			}
			return result;
		}
	}

	static class ExpectationStep
			implements
			Function<FeatureIntegerVectorClusterDoubleVector, FeatureIntegerVectorClusterDoubleVector> {
		protected final DoubleMatrix[] modelParameters;
		protected final int[] V;

		// protected final List<Integer> indexList;

		ExpectationStep(DoubleMatrix[] modelParameters, int[] V) {
			this.V = V;
			this.modelParameters = modelParameters;
			// this.indexList = new ArrayList<Integer>(V.length);
			// for (int d = 0; d < V.length; d++) {
			// this.indexList.add(d);
			// }

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
		public FeatureIntegerVectorClusterDoubleVector call(
				FeatureIntegerVectorClusterDoubleVector p) {
			DoubleMatrix clusterProbability = modelParameters[V.length].dup();

			// Collections.shuffle(indexList);
			// for (int d : indexList) {
			for (int d = 0; d < V.length; d++) {
				int featureValue = p.getFeatureValue(d);

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

			p.setClusterProbability(clusterProbability.data);

			return p;
		}
	}

	public static void main(String[] args) {
		NaiveBayesOptions optionsNaiveBayes = new NaiveBayesOptions(args);

		String inputPath = optionsNaiveBayes.getInputPath();
		String outputPath = optionsNaiveBayes.getOutputPath();
		int numberOfClusters = optionsNaiveBayes.getNumberOfClusters();
		int numberOfFeatures = optionsNaiveBayes.getNumberOfFeatures();
		int numberOfIterations = optionsNaiveBayes.getNumberOfIterations();
		double alpha = optionsNaiveBayes.getAlpha();
		if (alpha <= 0) {
			alpha = 1.0 / numberOfClusters;
		}

		int[] V = new int[numberOfFeatures];
		double[] beta = new double[numberOfFeatures];

		sLogger.info("Tool: " + NaiveBayes1.class.getSimpleName());
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of clusters: " + numberOfClusters);
		sLogger.info(" - number of features: " + numberOfFeatures);
		sLogger.info(" - number of iterations: " + numberOfIterations);
		sLogger.info(" - alpha: " + alpha);

		SparkConf sparkConf = new SparkConf().setAppName(optionsNaiveBayes
				.toString());

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(inputPath);
		JavaRDD<FeatureIntegerVectorClusterDoubleVector> points = lines.map(
				new ParseLine(numberOfFeatures)).cache();
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
		FeatureIntegerVectorClusterDoubleVector dataPoint;

		for (int i = 0; i < numberOfIterations; i++) {
			sLogger.info("On iteration " + (i + 1));

			// JavaPairRDD<Entry<Integer, Integer>, double[]> modelParameters =
			// points.flatMapToPair(
			// new ExpectationStepTrain(modelParameter, V)).reduceByKey(new
			// MaximizationStep());

			JavaRDD<FeatureIntegerVectorClusterDoubleVector> newPoints = points
					.map(new ExpectationStep(modelParameter, V)).cache();
			// points.unpersist();
			points = newPoints;

			DoubleMatrix[] newModelParameter = NaiveBayes3Long.uniformModel(alpha,
					beta, V, numberOfClusters);

			Iterator<FeatureIntegerVectorClusterDoubleVector> iterDataPoint = points
					.collect().iterator();
			while (iterDataPoint.hasNext()) {
				dataPoint = iterDataPoint.next();

				for (int d = 0; d < numberOfFeatures; d++) {
					int featureValue = dataPoint.getFeatureValue(d);

					if (featureValue <= -1) {
						continue;
					}

					if (featureValue >= V[d]) {
						throw new IllegalArgumentException("unexpected value "
								+ d + " of feature " + featureValue + "...");
					}

					for (int k = 0; k < numberOfClusters; k++) {
						newModelParameter[d].put(k, featureValue,
								newModelParameter[d].get(k, featureValue)
										+ dataPoint.getClusterProbability(k));
					}
				}

				for (int k = 0; k < numberOfClusters; k++) {
					newModelParameter[numberOfFeatures].put(k, 0,
							newModelParameter[numberOfFeatures].get(k, 0)
									+ dataPoint.getClusterProbability(k));
				}
			}

			for (int d = 0; d < numberOfFeatures; d++) {
				newModelParameter[d].diviColumnVector(newModelParameter[d]
						.rowSums());
			}
			newModelParameter[numberOfFeatures]
					.divi(newModelParameter[numberOfFeatures].sum());

			modelParameter = newModelParameter;
		}

		Utility.printJavaRDD(points);

		sc.stop();
	}
}