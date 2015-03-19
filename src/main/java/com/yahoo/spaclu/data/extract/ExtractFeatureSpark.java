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

package com.yahoo.spaclu.data.extract;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.yahoo.spaclu.model.Settings;

/**
 * Format raw textual data to database format
 */
public class ExtractFeatureSpark {
	final static Logger sLogger = LoggerFactory
			.getLogger(ExtractFeatureSpark.class);

	public static List<Entry<String, String>> patternString = new LinkedList<Entry<String, String>>();
	static {
		patternString.add(new SimpleEntry<String, String>(
				"(?i)(<strong.*?>)(.+?)(</strong>)", "$2"));

		// remove all the website patterns
		patternString.add(new SimpleEntry<String, String>(
				"(?i)(www\\.)(.+?)(\\.(com|net|gov|org|edu|co|biz|tv|info))",
				"$2"));
		patternString.add(new SimpleEntry<String, String>(
				"(?i)(.+?)(\\.(com|net|gov|org|edu|co|biz|tv|info))", "$1"));
		// remove all punctuations
		patternString.add(new SimpleEntry<String, String>("\\p{Punct}", " "));
		// remove all digits
		patternString.add(new SimpleEntry<String, String>("\\d+", " "));
		// remove all duplicate spaces
		patternString.add(new SimpleEntry<String, String>(" +", " "));
	}

	public enum CBBingCombinedFields {
		CBBingCombined_pvid, //
		CBBingCombined_sec, //
		CBBingCombined_adRank, //
		CBBingCombined_page_position, //
		CBBingCombined_dude, //
		CBBingCombined_n_ads, //
		CBBingCombined_pagenum, //
		CBBingCombined_rawQuery, //
		CBBingCombined_canonicalQuery, //
		CBBingCombined_exactCanonicalQuery, //
		CBBingCombined_osqp, //
		CBBingCombined_bucket, //
		CBBingCombined_timestamp, //
		CBBingCombined_matchType, //
		CBBingCombined_title, //
		CBBingCombined_description, //
		CBBingCombined_displayUrl, //
		CBBingCombined_targetUrl, //
		CBBingCombined_sitelinkNum, //
		CBBingCombined_sitelinkClicks, //
		CBBingCombined_adClicks, //
		CBBingCombined_adSiteLinkClicks, //
		CBBingCombined_clkbMS, //
		CBBingCombined_ec, //
		CBBingCombined_cpcMl, //
		CBBingCombined_cpcSb, //
		CBBingCombined_bid, //
		CBBingCombined_rankScore, //
		CBBingCombined_relevanceScore, //
		CBBingCombined_fddEcpm, //
		CBBingCombined_quality_score, //
		CBBingCombined_dwell_time, //
		CBBingCombined_bcookie, //
		CBBingCombined_ip, //
		CBBingCombined_birthYear, //
		CBBingCombined_gender, //
		CBBingCombined_browser, //
		CBBingCombined_userAgent, //
		CBBingCombined_referUrl, //
		CBBingCombined_ucp, //
		CBBingCombined_scb, //
		CBBingCombined_effectiveFddThresholds, //
		CBBingCombined_iguid, //
		CBBingCombined_model, //
		CBBingCombined_device_type, //
		CBBingCombined_screen_width, //
		CBBingCombined_screen_height, //
		CBBingCombined_carrier, //
		CBBingCombined_srcspc, //
		CBBingCombined_advertiser_id, //
		CBBingCombined_campaign_id, //
		CBBingCombined_adgroup_id, //
		CBBingCombined_ad_id, //
		CBBingCombined_bidded_term_id, //
		CBBingCombined_apollo_advertiser_id, //
		CBBingCombined_apollo_campaign_id, //
		CBBingCombined_apollo_adgroup_id, //
		CBBingCombined_apollo_ad_id, //
		CBBingCombined_apollo_bidded_term_id, //
		CBBingCombined_NormalizedKeyword, //
		CBBingCombined_ClickabilityScore, //
		CBBingCombined_deviceCategory, //
		CBBingCombined_source, //
		SIZE
	}

	final static class LineFilter implements Function<String, String> {
		protected final List<Integer> featureIndices;

		LineFilter(List<Integer> featureIndices) {
			this.featureIndices = featureIndices;
		}

		@Override
		public String call(String line) {
			// StringTokenizer stk = new StringTokenizer(line, Settings.TAB);
			String[] tokens = line.split(Settings.TAB);

			Preconditions.checkArgument(
					tokens.length == CBBingCombinedFields.SIZE.ordinal(),
					tokens.length + "\t" + CBBingCombinedFields.SIZE.ordinal());

			StringBuilder stringBuilder = new StringBuilder();
			Iterator<Integer> iter = featureIndices.iterator();
			while (iter.hasNext()) {
				String temp = tokens[iter.next()].toLowerCase();
				temp = StringEscapeUtils.unescapeXml(temp);
				Iterator<Entry<String, String>> patternIter = patternString
						.iterator();
				while (patternIter.hasNext()) {
					Entry<String, String> pattern = patternIter.next();
					temp = temp
							.replaceAll(pattern.getKey(), pattern.getValue());
				}
				stringBuilder.append(temp);
				stringBuilder.append(Settings.TAB);
			}

			return stringBuilder.toString().trim();
		}
	}

	public static void main(String[] args) throws IOException {
		ExtractFeatureOptions optionsFormatRawToDatabase = new ExtractFeatureOptions(
				args);

		String inputPathString = optionsFormatRawToDatabase.getInputPath();
		String outputPathString = optionsFormatRawToDatabase.getOutputPath();
		// String indexPathString = optionsFormatRawToDatabase.getIndexPath();
		int numberOfPartitions = optionsFormatRawToDatabase
				.getNumberOfPartitions();
		// int maxCutoffThreshold = optionsFormatRawToDatabase
		// .getMaximumCutoffThreshold();
		// int minCutoffThreshold = optionsFormatRawToDatabase
		// .getMinimumCutoffThreshold();

		/*
		 * Set<String> excludingFeatureNames = new HashSet<String>();
		 * excludingFeatureNames.add("login");
		 * excludingFeatureNames.add("time"); excludingFeatureNames.add("day");
		 * excludingFeatureNames.add("hms"); excludingFeatureNames.add("fail");
		 */

		sLogger.info("Tool: " + ExtractFeatureSpark.class.getSimpleName());
		sLogger.info(" - input path: " + inputPathString);
		sLogger.info(" - output path: " + outputPathString);
		// sLogger.info(" - index path: " + indexPathString);
		sLogger.info(" - number of partitions: " + numberOfPartitions);
		// sLogger.info(" - maximum cutoff: " + maxCutoffThreshold);
		// sLogger.info(" - minimum cutoff: " + minCutoffThreshold);

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

		List<Integer> listOfFeatureIndices = new LinkedList<Integer>();
		listOfFeatureIndices
				.add(CBBingCombinedFields.CBBingCombined_exactCanonicalQuery
						.ordinal());
		listOfFeatureIndices.add(CBBingCombinedFields.CBBingCombined_title
				.ordinal());
		listOfFeatureIndices
				.add(CBBingCombinedFields.CBBingCombined_description.ordinal());
		listOfFeatureIndices.add(CBBingCombinedFields.CBBingCombined_displayUrl
				.ordinal());
		listOfFeatureIndices.add(CBBingCombinedFields.CBBingCombined_targetUrl
				.ordinal());

		JavaRDD<String> rawLines = sc.textFile(inputPathString).repartition(
				numberOfPartitions);

		JavaRDD<String> tokenizedLines = rawLines.map(new LineFilter(
				listOfFeatureIndices));
		tokenizedLines.saveAsTextFile(outputPathString);

		/*
		 * Iterator<String[]> strArrIter = tokenizedLines.collect().iterator();
		 * while(strArrIter.hasNext()){
		 * sLogger.info(Arrays.toString(strArrIter.next())); }
		 */

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