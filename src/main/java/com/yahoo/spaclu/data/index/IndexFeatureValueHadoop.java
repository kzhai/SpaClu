package com.yahoo.spaclu.data.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;

import com.google.common.base.Preconditions;
import com.yahoo.spaclu.data.structure.ArrayOfIntWritable;

import edu.umd.cloud9.io.FileMerger;
import edu.umd.cloud9.io.pair.PairOfIntString;

public class IndexFeatureValueHadoop extends Configured implements Tool {
	static final Logger sLogger = Logger
			.getLogger(IndexFeatureValueHadoop.class);

	/*
	 * public static final String DOCUMENT = "document"; public static final
	 * String TERM = "term"; public static final String TITLE = "title"; public
	 * static final String INDEX = "index";
	 */

	public static final String TEMP = "temp";

	public static final String DATA = "data";

	public static final String MINIMUM_DOCUMENT_FREQUENCY = "minimumdocumentfrequency";
	public static final String MAXIMUM_DOCUMENT_FREQUENCY = "maximumdocumentfrequency";
	public static final String MINIMUM_TERM_FREQUENCY = "minimumtermfrequency";
	public static final String MAXIMUM_TERM_FREQUENCY = "maximumtermfrequency";

	public static final float DEFAULT_MINIMUM_DOCUMENT_FREQUENCY = 0.0f;
	public static final float DEFAULT_MAXIMUM_DOCUMENT_FREQUENCY = 1.0f;
	public static final float DEFAULT_MINIMUM_TERM_FREQUENCY = 0.0f;
	public static final float DEFAULT_MAXIMUM_TERM_FREQUENCY = 1.0f;

	/*
	 * private static enum MyCounter { TOTAL_TYPES, // total number of distinct
	 * words in all documents across // all languages TOTAL_TERMS, // total
	 * number of words in all documents across all // languages TOTAL_DOCS, //
	 * total number of write-ups appeared in the corpus TOTAL_ARTICLES, // total
	 * number of write-ups appeared in the corpus // across all languages
	 * TOTAL_COLLAPSED_DOCS, // total number of collapsed documents during //
	 * indexing }
	 */

	// TODO: change the counter names
	/*
	 * public static final String TOTAL_DOCS_IN_LANGUAGE =
	 * "Total Documents in Language"; public static final String
	 * LEFT_OVER_DOCS_IN_LANGUAGE = "Left-over Documents in Language"; public
	 * static final String COLLAPSED_DOCS_IN_LANGUAGE =
	 * "Collapsed Documents in Language"; public static final String
	 * TOTAL_DOCS_WITH_MISSING_LANGUAGES =
	 * "Total Documents with Missing Languages"; public static final String
	 * LOW_DOCUMENT_FREQUENCY_TERMS_IN_LANGUAGE = "Low DF Terms in Language";
	 * public static final String HIGH_DOCUMENT_FREQUENCY_TERMS_IN_LANGUAGE =
	 * "High DF Terms in Language"; public static final String
	 * LEFT_OVER_TERMS_IN_LANGUAGE = "Left-over Terms in Language";
	 */

	public static final String NUMBER_OF_VALUES_IN_FEATURE = "Total Terms in Language";

	public static final String NULL = "null";

	@SuppressWarnings("unchecked")
	public int run(String[] args) throws Exception {
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

		sLogger.info("Tool: " + IndexFeatureValueHadoop.class.getSimpleName());
		sLogger.info(" - input path: " + inputPathString);
		sLogger.info(" - output path: " + outputPathString);
		sLogger.info(" - index path: " + indexPathString);
		sLogger.info(" - number of partitions: " + numberOfPartitions);
		sLogger.info(" - maximum cutoff: " + maxCutoffThreshold);
		sLogger.info(" - minimum cutoff: " + minCutoffThreshold);

		int numberOfMappers = numberOfPartitions;
		int numberOfReducers = numberOfPartitions;

		// Create a default hadoop configuration
		Configuration configuration = this.getConf();

		// Parse created config to the HDFS
		FileSystem fs = FileSystem.get(configuration);

		// Delete the output directory if it exists already
		fs.delete(new Path(outputPathString), true);

		String indexPath = outputPathString + TEMP;

		try {
			/*
			 * SparkConf sparkConf = new SparkConf()
			 * .setAppName(optionsFormatRawToDatabase.toString());
			 * 
			 * JavaSparkContext sc = new JavaSparkContext(sparkConf);
			 * 
			 * Map<Integer, String> featureIndices = IndexFeatureValue
			 * .getFeatureIndices(sc.textFile(indexPathString));
			 * 
			 * int numberOfFeatures = featureIndices.size();
			 */

			BufferedReader bufferedReader = null;
			Map<Integer, String> featureIndices;
			try {
				bufferedReader = new BufferedReader((new InputStreamReader(
						fs.open(new Path(indexPathString)))));
				featureIndices = getFeatureIndices(bufferedReader);
			} finally {
				bufferedReader.close();
			}

			int numberOfFeatures = featureIndices.size();

			/*
			 * 
			 * 
			 * 	
			 */

			int[] featureValueCount = tokenizeDocument(configuration,
					inputPathString, indexPath, numberOfFeatures,
					numberOfMappers, numberOfReducers);
			Preconditions.checkArgument(
					featureValueCount.length == numberOfFeatures,
					"Unexpected term counts array...");
			sLogger.info("Features value count: "
					+ Arrays.toString(featureValueCount));

			/*
			 * 
			 * 
			 * 
			 */

			String outputFeatureNamePathString = outputPathString
					+ "feature.dat";
			Path outputFeatureNamePath = new Path(outputFeatureNamePathString);
			PrintWriter featureNamePrinterWriter = new PrintWriter(
					fs.create(outputFeatureNamePath), true);

			List<Integer> featuresToKeep = new LinkedList<Integer>();
			for (int i = 0; i < featureValueCount.length; i++) {
				if (featureValueCount[i] > maxCutoffThreshold
						|| featureValueCount[i] < minCutoffThreshold) {
					continue;
				}

				featuresToKeep.add(i);
				featureNamePrinterWriter.println(featureIndices.get(i));
			}
			featureNamePrinterWriter.close();

			sLogger.info("Features to keep: "
					+ Arrays.toString(featuresToKeep.toArray()));

			/*
			 * float[] minimumDocumentCount = new float[documentCount.length];
			 * float[] maximumDocumentCount = new float[documentCount.length];
			 * for (int featureIndex = 0; featureIndex < documentCount.length;
			 * featureIndex++) { minimumDocumentCount[featureIndex] =
			 * documentCount[featureIndex] minimumDocumentFrequency;
			 * maximumDocumentCount[featureIndex] = documentCount[featureIndex]
			 * maximumDocumentFrequency; }
			 */

			/*
			 * String titleGlobString = indexPath + Path.SEPARATOR + TITLE +
			 * Settings.UNDER_SCORE + TITLE + Settings.DASH + Settings.STAR;
			 * String titleString = outputPathString + TITLE; // Path
			 * titleIndexPath = indexTitle(titleGlobString, titleString, //
			 * numberOfMappers); Path titleIndexPath = null; titleIndexPath =
			 * indexTitle(configuration, titleGlobString, titleString,
			 * numberOfMappers);
			 */

			String termGlobString = indexPath + Path.SEPARATOR + "part-"
					+ Settings.STAR;
			String termString = outputPathString + Settings.FEATURE_INDICATOR;
			Path[] termIndexPath = indexFeatureValueDriver(configuration,
					termGlobString, termString, numberOfMappers,
					numberOfFeatures, featuresToKeep);

			String documentString = outputPathString + DATA;
			Path documentPath = indexDocumentDriver(configuration,
					inputPathString, documentString, termString,
					numberOfMappers, numberOfFeatures, featuresToKeep);
		} finally {
			fs.delete(new Path(indexPath), true);
		}

		return 0;
	}

	public static class TokenizeMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, PairOfIntString, IntWritable> {
		private PairOfIntString term = new PairOfIntString();
		private IntWritable counts = new IntWritable(1);

		@SuppressWarnings("deprecation")
		public void map(LongWritable key, Text value,
				OutputCollector<PairOfIntString, IntWritable> output,
				Reporter reporter) throws IOException {

			String[] tokens = value.toString().split(Settings.TAB);
			for (int i = 0; i < tokens.length; i++) {
				if (tokens[i] == null || tokens[i].length() == 0) {
					continue;
				}
				term.set(i, tokens[i]);
				output.collect(term, counts);
			}
		}
	}

	public static class TokenizeCombiner extends MapReduceBase implements
			Reducer<PairOfIntString, IntWritable, PairOfIntString, IntWritable> {
		private IntWritable counts = new IntWritable();

		public void reduce(PairOfIntString key, Iterator<IntWritable> values,
				OutputCollector<PairOfIntString, IntWritable> output,
				Reporter reporter) throws IOException {
			int documentFrequency = 0;

			while (values.hasNext()) {
				counts = values.next();
				documentFrequency += counts.get();
			}

			counts.set(documentFrequency);
			output.collect(key, counts);
		}
	}

	public static class TokenizeReducer extends MapReduceBase implements
			Reducer<PairOfIntString, IntWritable, PairOfIntString, IntWritable> {
		private IntWritable counts = new IntWritable();

		public void reduce(PairOfIntString key, Iterator<IntWritable> values,
				OutputCollector<PairOfIntString, IntWritable> output,
				Reporter reporter) throws IOException {
			int documentFrequency = 0;

			while (values.hasNext()) {
				counts = values.next();
				documentFrequency += counts.get();
			}

			counts.set(documentFrequency);
			output.collect(key, counts);

			reporter.incrCounter(NUMBER_OF_VALUES_IN_FEATURE,
					"" + key.getLeftElement(), 1);
		}
	}

	public int[] tokenizeDocument(Configuration configuration,
			String inputPath, String outputPath, int numberOfFeatures,
			int numberOfMappers, int numberOfReducers) throws Exception {
		sLogger.info("Tool: " + IndexFeatureValueHadoop.class.getSimpleName()
				+ " - tokenize document");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of languages: " + numberOfFeatures);
		sLogger.info(" - number of mappers: " + numberOfMappers);
		sLogger.info(" - number of reducers: " + numberOfReducers);

		JobConf conf = new JobConf(configuration, IndexFeatureValueHadoop.class);
		conf.setJobName(IndexFeatureValueHadoop.class.getSimpleName()
				+ " - tokenize document");
		FileSystem fs = FileSystem.get(conf);

		/*
		 * MultipleOutputs.addMultiNamedOutput(conf, DOCUMENT,
		 * SequenceFileOutputFormat.class, Text.class, ArrayListWritable.class);
		 * MultipleOutputs.addMultiNamedOutput(conf, TITLE,
		 * SequenceFileOutputFormat.class, Text.class, NullWritable.class);
		 * 
		 * conf.setInt(Settings.PROPERTY_PREFIX + "model.languages",
		 * numberOfLanguages);
		 */

		conf.setNumMapTasks(numberOfMappers);
		conf.setNumReduceTasks(numberOfReducers);

		conf.setMapperClass(TokenizeMapper.class);
		conf.setReducerClass(TokenizeReducer.class);
		conf.setCombinerClass(TokenizeCombiner.class);

		conf.setMapOutputKeyClass(PairOfIntString.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(PairOfIntString.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, true);

		long startTime = System.currentTimeMillis();
		RunningJob job = JobClient.runJob(conf);
		sLogger.info("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		Counters counters = job.getCounters();

		int[] featureValueCount = new int[numberOfFeatures];
		for (int featureIndex = 0; featureIndex < numberOfFeatures; featureIndex++) {
			featureValueCount[featureIndex] = (int) counters.findCounter(
					NUMBER_OF_VALUES_IN_FEATURE, "" + featureIndex)
					.getCounter();
			sLogger.info("Total number of terms in language " + featureIndex
					+ " is: " + featureValueCount[featureIndex]);
		}

		return featureValueCount;
	}

	/**
	 * @deprecated
	 * @param configuration
	 * @param inputTitles
	 * @param outputTitle
	 * @param numberOfMappers
	 * @return
	 * @throws Exception
	 */
	public Path indexTitle(Configuration configuration, String inputTitles,
			String outputTitle, int numberOfMappers) throws Exception {
		sLogger.info("Tool: " + IndexFeatureValueHadoop.class.getSimpleName()
				+ " - index title");
		sLogger.info(" - input path: " + inputTitles);
		sLogger.info(" - output path: " + outputTitle);
		sLogger.info(" - number of mappers: " + numberOfMappers);

		JobConf conf = new JobConf(configuration, IndexFeatureValueHadoop.class);
		FileSystem fs = FileSystem.get(conf);

		Path titleIndexPath = new Path(outputTitle);

		String outputTitleFile = titleIndexPath.getParent() + Path.SEPARATOR
				+ Settings.TEMP + FileMerger.generateRandomString();

		// TODO: filemerger local merge can not deal with NullWritable class
		Path titlePath = FileMerger.mergeSequenceFiles(configuration,
				inputTitles, outputTitleFile, numberOfMappers, Text.class,
				NullWritable.class, true);

		SequenceFile.Reader sequenceFileReader = null;
		SequenceFile.Writer sequenceFileWriter = null;
		fs.createNewFile(titleIndexPath);
		try {
			sequenceFileReader = new SequenceFile.Reader(fs, titlePath, conf);
			sequenceFileWriter = new SequenceFile.Writer(fs, conf,
					titleIndexPath, IntWritable.class, Text.class);
			exportTitles(sequenceFileReader, sequenceFileWriter);
			sLogger.info("Successfully index all the titles to "
					+ titleIndexPath);
		} finally {
			IOUtils.closeStream(sequenceFileReader);
			IOUtils.closeStream(sequenceFileWriter);
			fs.delete(new Path(outputTitleFile), true);
		}

		return titleIndexPath;
	}

	public static class IndexFeatureValueMapper extends MapReduceBase implements
			Mapper<PairOfIntString, IntWritable, IntWritable, PairOfIntString> {
		private Hashtable<Integer, Integer> featuresIndexMapping;

		// private float[] minimumDocumentCount = null;
		// private float[] maximumDocumentCount = null;

		private IntWritable outputKey = new IntWritable();
		private PairOfIntString outputValue = new PairOfIntString();

		@SuppressWarnings("deprecation")
		public void map(PairOfIntString key, IntWritable value,
				OutputCollector<IntWritable, PairOfIntString> output,
				Reporter reporter) throws IOException {
			if (!featuresIndexMapping.containsKey(key.getLeftElement())) {
				return;
			}

			/*
			 * if (value.getLeftElement() < minimumDocumentCount[featureIndex])
			 * { reporter.incrCounter(LOW_DOCUMENT_FREQUENCY_TERMS_IN_LANGUAGE,
			 * "" + featureIndex, 1); return; } if (value.getLeftElement() >
			 * maximumDocumentCount[featureIndex]) {
			 * reporter.incrCounter(HIGH_DOCUMENT_FREQUENCY_TERMS_IN_LANGUAGE,
			 * "" + featureIndex, 1); return; }
			 */

			outputKey.set(featuresIndexMapping.get(key.getLeftElement()));
			outputValue.set(value.get(), key.getRightElement());
			output.collect(outputKey, outputValue);
		}

		public void configure(JobConf conf) {
			String[] featuresToKeepString = conf.get("features.to.keep", null)
					.split(Settings.SPACE);
			TreeSet<Integer> featuresToKeep = new TreeSet<Integer>();
			for (int i = 0; i < featuresToKeepString.length; i++) {
				featuresToKeep.add(Integer.valueOf(featuresToKeepString[i]));
			}

			featuresIndexMapping = new Hashtable<Integer, Integer>();
			Iterator<Integer> intIter = featuresToKeep.iterator();
			while (intIter.hasNext()) {
				featuresIndexMapping.put(intIter.next(),
						featuresIndexMapping.size());
			}

			/*
			 * minimumDocumentCount = new float[numberOfFeatures + 1];
			 * maximumDocumentCount = new float[numberOfFeatures + 1]; for (int
			 * languageIndex = 0; languageIndex < minimumDocumentCount.length;
			 * languageIndex++) { minimumDocumentCount[languageIndex] =
			 * conf.getFloat( Settings.PROPERTY_PREFIX +
			 * "corpus.minimum.document.count.language" + languageIndex, 0);
			 * maximumDocumentCount[languageIndex] = conf.getFloat(
			 * Settings.PROPERTY_PREFIX +
			 * "corpus.maximum.document.count.language" + languageIndex,
			 * Float.MAX_VALUE); }
			 */
		}
	}

	public static class IndexFeatureValueReducer extends MapReduceBase
			implements Reducer<IntWritable, PairOfIntString, IntWritable, Text> {

		private MultipleOutputs multipleOutputs = null;

		private PairOfIntString pairOfIntString;
		private IntWritable outputKey = new IntWritable();
		private Text outputValue = new Text();

		private OutputCollector<IntWritable, Text> outputTerm;

		@SuppressWarnings("deprecation")
		public void reduce(IntWritable key, Iterator<PairOfIntString> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			outputTerm = multipleOutputs.getCollector(Settings.INDEX_INDICATOR,
					"" + key.get(), reporter);

			int count = 0;
			while (values.hasNext()) {
				pairOfIntString = values.next();
				outputKey.set(count);
				outputValue.set(pairOfIntString.getRightElement());
				outputTerm.collect(outputKey, outputValue);

				count += 1;
			}
		}

		public void configure(JobConf conf) {
			multipleOutputs = new MultipleOutputs(conf);
		}

		public void close() throws IOException {
			multipleOutputs.close();
		}
	}

	public static int exportTitles(SequenceFile.Reader sequenceFileReader,
			SequenceFile.Writer sequenceWriter) throws IOException {
		Text text = new Text();
		IntWritable intWritable = new IntWritable();
		int index = 0;
		while (sequenceFileReader.next(text)) {
			index++;
			intWritable.set(index);
			sequenceWriter.append(intWritable, text);
		}

		return index;
	}

	public Path[] indexFeatureValueDriver(Configuration configuration,
			String inputTerms, String outputTerm, int numberOfMappers,
			int numberOfFeatures, Iterable<Integer> featureToKeep)
			throws Exception {
		sLogger.info("Tool: " + IndexFeatureValueHadoop.class.getSimpleName()
				+ " - index term");
		sLogger.info(" - input path: " + inputTerms);
		sLogger.info(" - output path: " + outputTerm);
		sLogger.info(" - number of features: " + numberOfFeatures);
		sLogger.info(" - number of mappers: " + numberOfMappers);
		sLogger.info(" - number of reducers: " + 1);
		// sLogger.info(" - minimum document count: " + minimumDocumentCount);
		// sLogger.info(" - maximum document count: " + maximumDocumentCount);

		Path inputTermFiles = new Path(inputTerms);
		Path[] outputTermFile = new Path[numberOfFeatures];

		JobConf conf = new JobConf(configuration, IndexFeatureValueHadoop.class);
		FileSystem fs = FileSystem.get(conf);

		conf.setJobName(IndexFeatureValueHadoop.class.getSimpleName()
				+ " - index term");

		conf.setInt(Settings.PROPERTY_PREFIX + "model.languages",
				numberOfFeatures);

		Iterator<Integer> intIter = featureToKeep.iterator();
		StringBuilder featuresToKeepStringBuilder = new StringBuilder();
		while (intIter.hasNext()) {
			featuresToKeepStringBuilder.append(intIter.next());
			featuresToKeepStringBuilder.append(Settings.SPACE);
		}
		conf.set("features.to.keep", featuresToKeepStringBuilder.toString()
				.trim());

		/*
		 * for (int languageIndex = 0; languageIndex <
		 * minimumDocumentCount.length; languageIndex++) {
		 * conf.setFloat(Settings.PROPERTY_PREFIX +
		 * "corpus.minimum.document.count.language" + languageIndex,
		 * minimumDocumentCount[languageIndex]);
		 * conf.setFloat(Settings.PROPERTY_PREFIX +
		 * "corpus.maximum.document.count.language" + languageIndex,
		 * maximumDocumentCount[languageIndex]); }
		 */

		conf.setNumMapTasks(numberOfMappers);
		conf.setNumReduceTasks(numberOfFeatures);
		conf.setMapperClass(IndexFeatureValueMapper.class);
		conf.setReducerClass(IndexFeatureValueReducer.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(PairOfIntString.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		MultipleOutputs.addMultiNamedOutput(conf, Settings.INDEX_INDICATOR,
				TextOutputFormat.class, IntWritable.class, Text.class);

		conf.setInputFormat(TextInputFormat.class);
		// suppress the empty files
		conf.setOutputFormat(NullOutputFormat.class);

		String outputString = (new Path(outputTerm)).getParent()
				+ Path.SEPARATOR + Settings.FEATURE_INDICATOR;
		Path outputPath = new Path(outputString);
		fs.delete(outputPath, true);

		FileInputFormat.setInputPaths(conf, inputTermFiles);
		FileOutputFormat.setOutputPath(conf, outputPath);
		FileOutputFormat.setCompressOutput(conf, true);

		long startTime = System.currentTimeMillis();
		RunningJob job = JobClient.runJob(conf);
		sLogger.info("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		/*
		 * try {
		 * 
		 * } finally { fs.delete(outputPath, true); }
		 */

		return outputTermFile;
	}

	public static class IndexDocumentMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, ArrayOfIntWritable> {
		private static Hashtable<Integer, Integer> featuresIndexMapping;

		private static Map<String, Integer>[] termIndex = null;

		private static int[] featureVector;

		private ArrayOfIntWritable outputValue = new ArrayOfIntWritable();

		@SuppressWarnings("deprecation")
		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, ArrayOfIntWritable> output,
				Reporter reporter) throws IOException {
			for (int i = 0; i < featureVector.length; i++) {
				featureVector[i] = -1;
			}

			String[] tokens = value.toString().split(Settings.TAB);
			for (int featureIndex = 0; featureIndex < tokens.length; featureIndex++) {
				if (!featuresIndexMapping.containsKey(featureIndex)) {
					continue;
				}

				int newFeatureIndex = featuresIndexMapping.get(featureIndex);

				featureVector[newFeatureIndex] = termIndex[newFeatureIndex]
						.get(tokens[featureIndex]);
			}

			outputValue.setArray(featureVector);
			output.collect(NullWritable.get(), outputValue);
		}

		@SuppressWarnings("deprecation")
		public void configure(JobConf conf) {
			String[] featuresToKeepString = conf.get("features.to.keep", null)
					.split(Settings.SPACE);
			TreeSet<Integer> featuresToKeep = new TreeSet<Integer>();
			for (int i = 0; i < featuresToKeepString.length; i++) {
				featuresToKeep.add(Integer.valueOf(featuresToKeepString[i]));
			}

			featuresIndexMapping = new Hashtable<Integer, Integer>();
			Iterator<Integer> intIter = featuresToKeep.iterator();
			while (intIter.hasNext()) {
				featuresIndexMapping.put(intIter.next(),
						featuresIndexMapping.size());
			}

			termIndex = new Map[featuresToKeep.size()];
			featureVector = new int[featuresToKeep.size()];

			SequenceFile.Reader sequenceFileReader = null;
			try {
				Path[] inputFiles = DistributedCache.getLocalCacheFiles(conf);
				// TODO: check for the missing columns...
				if (inputFiles != null) {
					for (Path path : inputFiles) {
						try {
							sequenceFileReader = new SequenceFile.Reader(
									FileSystem.getLocal(conf), path, conf);

							if (path.getName().startsWith(
									Settings.INDEX_INDICATOR)) {
								int languageIndex = Integer.parseInt(path
										.getName().substring(
												path.getName().indexOf(
														Settings.UNDER_SCORE)
														+ Settings.UNDER_SCORE
																.length()));

								termIndex[languageIndex] = importParameter(sequenceFileReader);
							} else {
								throw new IllegalArgumentException(
										"Unexpected file in distributed cache: "
												+ path.getName());
							}
						} catch (IllegalArgumentException iae) {
							iae.printStackTrace();
						} catch (IOException ioe) {
							ioe.printStackTrace();
						}
					}
				}
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} finally {
				IOUtils.closeStream(sequenceFileReader);
			}
		}
	}

	public Path indexDocumentDriver(Configuration configuration,
			String inputDocument, String outputDocument, String termIndex,
			int numberOfMappers, int numberOfFeatures,
			List<Integer> featuresToKeep) throws Exception {
		sLogger.info("Tool: " + IndexFeatureValueHadoop.class.getSimpleName()
				+ " - index document");
		sLogger.info(" - input path: " + inputDocument);
		sLogger.info(" - output path: " + outputDocument);
		sLogger.info(" - term index path: " + termIndex);
		sLogger.info(" - number of languages: " + numberOfFeatures);
		sLogger.info(" - number of mappers: " + numberOfMappers);
		sLogger.info(" - number of reducers: " + 0);

		JobConf conf = new JobConf(configuration, IndexFeatureValueHadoop.class);
		FileSystem fs = FileSystem.get(conf);

		conf.setJobName(IndexFeatureValueHadoop.class.getSimpleName()
				+ " - index document");

		Path inputDocumentFiles = new Path(inputDocument);
		Path outputDocumentFiles = new Path(outputDocument);

		Iterator<Integer> intIter = featuresToKeep.iterator();
		StringBuilder featuresToKeepStringBuilder = new StringBuilder();
		while (intIter.hasNext()) {
			featuresToKeepStringBuilder.append(intIter.next());
			featuresToKeepStringBuilder.append(Settings.SPACE);
		}
		conf.set("features.to.keep", featuresToKeepStringBuilder.toString()
				.trim());

		for (int featureIndex = 0; featureIndex < featuresToKeep.size(); featureIndex++) {
			String termIndexPathString = termIndex + Settings.SEPERATOR
					+ Settings.INDEX_INDICATOR + Settings.UNDER_SCORE
					+ featureIndex + Settings.DASH + Settings.STAR;

			FileStatus[] fileStatusArray = fs.globStatus(new Path(
					termIndexPathString));
			Preconditions.checkArgument(fileStatusArray.length == 1);

			Path tempTermIndexPath = fileStatusArray[0].getPath();

			Path termIndexPath = new Path(termIndex + Settings.SEPERATOR
					+ Settings.INDEX_INDICATOR + Settings.UNDER_SCORE
					+ featureIndex);
			fs.rename(tempTermIndexPath, termIndexPath);

			Preconditions.checkArgument(fs.exists(termIndexPath),
					"Missing term index files for language " + featureIndex
							+ "...");
			DistributedCache.addCacheFile(termIndexPath.toUri(), conf);
		}

		conf.setNumMapTasks(numberOfMappers);
		conf.setNumReduceTasks(0);
		conf.setMapperClass(IndexDocumentMapper.class);

		conf.setMapOutputKeyClass(NullWritable.class);
		conf.setMapOutputValueClass(ArrayOfIntWritable.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(ArrayOfIntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, inputDocumentFiles);
		FileOutputFormat.setOutputPath(conf, outputDocumentFiles);
		FileOutputFormat.setCompressOutput(conf, false);

		long startTime = System.currentTimeMillis();
		RunningJob job = JobClient.runJob(conf);
		sLogger.info("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		sLogger.info("Successfully index all the documents at "
				+ outputDocumentFiles);

		/*
		 * Counters counters = job.getCounters();
		 * 
		 * int articleCount = (int)
		 * counters.findCounter(MyCounter.TOTAL_ARTICLES) .getCounter();
		 * sLogger.info("Total number of articles across all languages is: " +
		 * articleCount); int collapsedDocumentCount = (int)
		 * counters.findCounter( MyCounter.TOTAL_COLLAPSED_DOCS).getCounter();
		 * sLogger.info("Total number of collapsed documents is: " +
		 * collapsedDocumentCount); int documentCount = (int)
		 * counters.findCounter(MyCounter.TOTAL_DOCS) .getCounter();
		 * sLogger.info("Total number of documents is: " + documentCount);
		 */

		return outputDocumentFiles;
	}

	public static Map<String, Integer> importParameter(
			SequenceFile.Reader sequenceFileReader) throws IOException {
		Map<String, Integer> hashMap = new HashMap<String, Integer>();

		IntWritable intWritable = new IntWritable();
		Text text = new Text();
		while (sequenceFileReader.next(intWritable, text)) {
			if (intWritable.get() % 100000 == 0) {
				sLogger.info("Imported term " + text.toString()
						+ " with index " + intWritable.toString());
			}
			hashMap.put(text.toString(), intWritable.get());
		}

		return hashMap;
	}

	public static Map<Integer, String> getFeatureIndices(
			BufferedReader bufferedReader) throws IOException {
		Map<Integer, String> hashMap = new HashMap<Integer, String>();

		String line = bufferedReader.readLine();
		while (line != null) {
			line = line.trim();
			String[] fieldNameTokens = Settings.SPACE_PATTERN.split(line);

			if (Integer.parseInt(fieldNameTokens[1]) == 0) {
				hashMap.put(hashMap.size(), line);
			}

			line = bufferedReader.readLine();
		}

		return hashMap;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new IndexFeatureValueHadoop(), args);
		System.exit(res);
	}
}