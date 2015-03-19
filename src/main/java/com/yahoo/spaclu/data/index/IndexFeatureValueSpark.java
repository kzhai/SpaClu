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

package com.yahoo.spaclu.data.index;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.AbstractMap.SimpleEntry;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedMap.Builder;
import com.google.common.collect.Ordering;

import com.yahoo.spaclu.data.structure.FeatureIntegerVector;

//import com.yahoo.spaclu.data.Utility;

/**
 * Format raw textual data to database format
 */
public class IndexFeatureValueSpark {
	final static Logger sLogger = LoggerFactory
			.getLogger(IndexFeatureValueSpark.class);

	final static class LineFilter implements Function<String, String[]> {
		protected final List<Integer> featurePositions;

		LineFilter(List<Integer> featurePositions) {
			this.featurePositions = featurePositions;
		}

		@Override
		public String[] call(String line) {
			String[] features = new String[featurePositions.size()];

			String[] tok = Settings.TAB_PATTERN.split(line.trim());
			for (int i = 0; i < features.length; i++) {
				features[i] = tok[featurePositions.get(i)];
			}

			return features;
		}
	}

	final static class FeatureValueMapper implements
			PairFlatMapFunction<String[], Entry<Integer, String>, Long> {
		LinkedList<Tuple2<Map.Entry<Integer, String>, Long>> linkedList;

		@Override
		public Iterable<Tuple2<Map.Entry<Integer, String>, Long>> call(
				String[] tok) {
			linkedList = new LinkedList<Tuple2<Entry<Integer, String>, Long>>();

			for (int i = 0; i < tok.length; i++) {
				if (tok[i].equals("")) {
					continue;
				}

				linkedList.add(new Tuple2<Entry<Integer, String>, Long>(
						new SimpleEntry<Integer, String>(i, tok[i]), 1l));
			}

			return linkedList;
		}
	}

	final static class FeatureValueReducer implements
			Function2<Long, Long, Long> {
		@Override
		public Long call(Long i1, Long i2) {
			return i1 + i2;
		}
	}

	final static class FeatureValueIndexer implements
			Function<String[], FeatureIntegerVector> {
		protected final Map<Integer, Map<String, Integer>> featureValueIndex;

		FeatureValueIndexer(Map<Integer, Map<String, Integer>> featureValueIndex) {
			this.featureValueIndex = featureValueIndex;
		}

		@Override
		public FeatureIntegerVector call(String[] tok) {
			Preconditions
					.checkArgument(this.featureValueIndex.size() == tok.length);

			int[] featureVector = new int[this.featureValueIndex.size()];

			for (int i = 0; i < tok.length; i++) {
				if (tok[i].equals("")) {
					featureVector[i] = -1;
				} else {
					featureVector[i] = this.featureValueIndex.get(i)
							.get(tok[i]);
				}
			}

			return new FeatureIntegerVector(featureVector);
		}
	}

	public static Map<Integer, String> getFeatureIndices(JavaRDD<String> data)
			throws IOException {
		Map<Integer, String> featureIndices = new Hashtable<Integer, String>();

		Iterator<String> fieldNamesIter = data.collect().iterator();
		int count = 0;
		while (fieldNamesIter.hasNext()) {
			String fieldNameString = fieldNamesIter.next();
			String[] fieldNameTokens = Settings.SPACE_PATTERN
					.split(fieldNameString);

			if (Integer.parseInt(fieldNameTokens[1]) == 0) {
				featureIndices.put(count, fieldNameString);
			}

			count += 1;
		}

		return featureIndices;
	}

	public static void main(String[] args) throws IOException {
		IndexFeatureValueOptions optionsFormatRawToDatabase = new IndexFeatureValueOptions(
				args);

		String inputPathString = optionsFormatRawToDatabase.getInputPath();
		String outputPathString = optionsFormatRawToDatabase.getOutputPath();
		String indexPathString = optionsFormatRawToDatabase.getIndexPath();
		int numberOfPartitions = optionsFormatRawToDatabase
				.getNumberOfPartitions();
		int maxCutoffThreshold = optionsFormatRawToDatabase
				.getMaximumCutoffThreshold();
		int minCutoffThreshold = optionsFormatRawToDatabase
				.getMinimumCutoffThreshold();

		/*
		 * Set<String> excludingFeatureNames = new HashSet<String>();
		 * excludingFeatureNames.add("login");
		 * excludingFeatureNames.add("time"); excludingFeatureNames.add("day");
		 * excludingFeatureNames.add("hms"); excludingFeatureNames.add("fail");
		 */

		sLogger.info("Tool: " + IndexFeatureValueSpark.class.getSimpleName());
		sLogger.info(" - input path: " + inputPathString);
		sLogger.info(" - output path: " + outputPathString);
		sLogger.info(" - index path: " + indexPathString);
		sLogger.info(" - number of partitions: " + numberOfPartitions);
		sLogger.info(" - maximum cutoff: " + maxCutoffThreshold);
		sLogger.info(" - minimum cutoff: " + minCutoffThreshold);

		// Create a default hadoop configuration
		Configuration conf = new Configuration();

		// Parse created config to the HDFS
		FileSystem fs = FileSystem.get(conf);

		Path outputPath = new Path(outputPathString);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		SparkConf sparkConf = new SparkConf()
				.setAppName(optionsFormatRawToDatabase.toString());

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		Map<Integer, String> featureIndices = getFeatureIndices(sc
				.textFile(indexPathString));

		List<Integer> listOfAllFeatureIndices = new LinkedList<Integer>();
		List<String> listOfAllFeatureInfo = new LinkedList<String>();
		Iterator<Integer> indexIter = featureIndices.keySet().iterator();
		while (indexIter.hasNext()) {
			Integer tempKey = indexIter.next();
			listOfAllFeatureIndices.add(tempKey);
			listOfAllFeatureInfo.add(featureIndices.get(tempKey));
		}

		/*
		 * 
		 * 
		 * 
		 * 
		 * 
		 * 
		 * 
		 */

		JavaRDD<String> rawLines = sc.textFile(inputPathString).repartition(
				numberOfPartitions);

		JavaRDD<String[]> tokenizedLines = rawLines.map(new LineFilter(
				listOfAllFeatureIndices));
		JavaPairRDD<Entry<Integer, String>, Long> featureValuesCounts = tokenizedLines
				.flatMapToPair(new FeatureValueMapper()).reduceByKey(
						new FeatureValueReducer());

		Map<Integer, Builder<String, Long>> featureValueMapping = new Hashtable<Integer, Builder<String, Long>>();
		Iterator<Tuple2<Entry<Integer, String>, Long>> iter = featureValuesCounts
				.collect().iterator();
		while (iter.hasNext()) {
			Tuple2<Entry<Integer, String>, Long> temp = iter.next();
			Entry<Integer, String> featureValueEntry = temp._1;
			int featureIndex = featureValueEntry.getKey();
			String featureValue = featureValueEntry.getValue();
			long featureValueCount = temp._2;

			if (!featureValueMapping.containsKey(featureIndex)) {
				Builder<String, Long> mapBuilder = new Builder<String, Long>(
						Ordering.natural());

				featureValueMapping.put(featureIndex, mapBuilder);
			}

			featureValueMapping.get(featureIndex).put(featureValue,
					featureValueCount);
		}

		Preconditions
				.checkArgument(featureValueMapping.size() == listOfAllFeatureIndices
						.size());

		String outputFeaturePathString = outputPathString + "feature"
				+ Settings.SEPERATOR;
		fs.mkdirs(new Path(outputFeaturePathString));

		String outputFeatureNamePathString = outputPathString + "feature.dat";
		Path outputFeatureNamePath = new Path(outputFeatureNamePathString);
		PrintWriter featureNamePrinterWriter = new PrintWriter(
				fs.create(outputFeatureNamePath), true);

		List<Integer> listOfFeatureIndicesToKeep = new LinkedList<Integer>();

		Map<Integer, Map<String, Integer>> featureValueIndex = new Hashtable<Integer, Map<String, Integer>>();
		for (int d = 0; d < featureValueMapping.size(); d++) {
			Map<String, Integer> valueToIndex = new Hashtable<String, Integer>();
			Map<Integer, String> indexToValue = new Hashtable<Integer, String>();

			ImmutableSortedMap<String, Long> immutableSortedMap = featureValueMapping
					.get(d).build();
			for (String keyString : immutableSortedMap.keySet()) {
				valueToIndex.put(keyString, valueToIndex.size());
				indexToValue.put(indexToValue.size(), keyString);
			}

			if (valueToIndex.size() <= minCutoffThreshold
					|| valueToIndex.size() > maxCutoffThreshold) {
				sLogger.info("Feature (" + listOfAllFeatureInfo.get(d)
						+ ") contains " + valueToIndex.size()
						+ " values, skip...");

				continue;
			} else {
				sLogger.info("Feature (" + listOfAllFeatureInfo.get(d)
						+ ") contains " + valueToIndex.size() + " values.");

				listOfFeatureIndicesToKeep.add(listOfAllFeatureIndices.get(d));
				featureNamePrinterWriter.println(listOfAllFeatureInfo.get(d));
			}

			String outputFeatureIndexPathString = outputFeaturePathString
					+ "index" + Settings.UNDER_SCORE + featureValueIndex.size()
					+ ".dat";
			Path outputIndexPath = new Path(outputFeatureIndexPathString);

			featureValueIndex.put(featureValueIndex.size(), valueToIndex);

			PrintWriter featureValueIndexPrinterWriter = new PrintWriter(
					fs.create(outputIndexPath), true);
			for (int i = 0; i < indexToValue.size(); i++) {
				featureValueIndexPrinterWriter.println("" + i + Settings.TAB
						+ indexToValue.get(i) + Settings.TAB
						+ immutableSortedMap.get(indexToValue.get(i)));
			}
			featureValueIndexPrinterWriter.close();
		}

		featureNamePrinterWriter.close();

		JavaRDD<String[]> filteredLines = rawLines.map(new LineFilter(
				listOfFeatureIndicesToKeep));
		JavaRDD<FeatureIntegerVector> indexedData = filteredLines
				.map(new FeatureValueIndexer(featureValueIndex));

		String outputDataPathString = outputPathString + "data";
		Path outputDataPath = new Path(outputDataPathString);
		if (fs.exists(outputDataPath)) {
			fs.delete(outputDataPath, true);
		}
		indexedData.saveAsTextFile(outputDataPathString);

		sc.stop();
	}

	/**
	 * @deprecated
	 */
	public static boolean writeToHDFS(Object object, String fileName) {
		// Create a default hadoop configuration
		Configuration conf = new Configuration();
		// Specifies a new file in HDFS.
		Path filenamePath = new Path(fileName);

		try {
			// Parse created config to the HDFS
			FileSystem fs = FileSystem.get(conf);

			// if the file already exists delete it.
			if (fs.exists(filenamePath)) {
				throw new IOException();
			}

			FSDataOutputStream fos = fs.create(filenamePath);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(object);
			fos.close();
			oos.close();
			return true;
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return false;
		}
	}
}