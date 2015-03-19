package com.yahoo.spaclu.data.index;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.yahoo.spaclu.data.index.IndexFeatureValueOptions;
import com.yahoo.spaclu.data.index.Settings;

public class IndexFeatureValueOptions {
	final Logger sLogger = LoggerFactory
			.getLogger(IndexFeatureValueOptions.class);

	private String inputPath = null;
	private String outputPath = null;
	private String indexPath = null;

	private int numberOfPartitions = 1000;

	private int minimumCutoffThreshold = 1;
	private int maximumCutoffThreshold = Integer.MAX_VALUE;

	public IndexFeatureValueOptions(String args[]) {
		Options options = new Options();
		options.addOption(Settings.HELP_OPTION, false, "print the help message");

		options.addOption(OptionBuilder.withArgName(Settings.PATH_INDICATOR)
				.hasArg().withDescription("input file or directory")
				.isRequired().create(Settings.INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName(Settings.PATH_INDICATOR)
				.hasArg().withDescription("output directory").isRequired()
				.create(Settings.OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName(Settings.PATH_INDICATOR)
				.hasArg().withDescription("index file path").isRequired()
				.create(Settings.INDEX_INDICATOR));

		options.addOption(OptionBuilder.withArgName(Settings.INTEGER_INDICATOR)
				.hasArg().withDescription("number of partitions")
				.create(Settings.PARTITION_OPTION));

		options.addOption(OptionBuilder.withArgName(Settings.INTEGER_INDICATOR)
				.hasArg().withDescription("maximum cutoff threshold")
				.create(Settings.MAX_CUTOFF_OPTION));
		options.addOption(OptionBuilder.withArgName(Settings.INTEGER_INDICATOR)
				.hasArg().withDescription("minimum cutoff threshold")
				.create(Settings.MIN_CUTOFF_OPTION));

		CommandLineParser parser = new GnuParser();
		HelpFormatter formatter = new HelpFormatter();
		try {
			CommandLine line = parser.parse(options, args);

			if (line.hasOption(Settings.HELP_OPTION)) {
				formatter.printHelp(IndexFeatureValueSpark.class.getName(),
						options);
				System.exit(0);
			}

			if (line.hasOption(Settings.INPUT_OPTION)) {
				inputPath = line.getOptionValue(Settings.INPUT_OPTION);
			}

			if (line.hasOption(Settings.OUTPUT_OPTION)) {
				outputPath = line.getOptionValue(Settings.OUTPUT_OPTION);

				if (!outputPath.endsWith(Settings.SEPERATOR)) {
					outputPath += Settings.SEPERATOR;
				}
			}

			if (line.hasOption(Settings.INDEX_INDICATOR)) {
				indexPath = line.getOptionValue(Settings.INDEX_INDICATOR);
			}

			if (line.hasOption(Settings.PARTITION_OPTION)) {
				numberOfPartitions = Integer.parseInt(line
						.getOptionValue(Settings.PARTITION_OPTION));
				Preconditions.checkArgument(numberOfPartitions > 0,
						"Illegal settings for " + Settings.PARTITION_OPTION
								+ " option: must be strictly positive...");
			}

			if (line.hasOption(Settings.MAX_CUTOFF_OPTION)) {
				maximumCutoffThreshold = Integer.parseInt(line
						.getOptionValue(Settings.MAX_CUTOFF_OPTION));
			}
			if (line.hasOption(Settings.MIN_CUTOFF_OPTION)) {
				minimumCutoffThreshold = Integer.parseInt(line
						.getOptionValue(Settings.MIN_CUTOFF_OPTION));
			}
		} catch (ParseException pe) {
			sLogger.error(pe.getMessage());
			formatter.printHelp(IndexFeatureValueHadoop.class.getName(),
					options);
			System.exit(0);
		} catch (NumberFormatException nfe) {
			sLogger.error(nfe.getMessage());
			nfe.printStackTrace(System.err);
			System.exit(0);
		} catch (IllegalArgumentException iae) {
			sLogger.error(iae.getMessage());
			iae.printStackTrace(System.err);
			System.exit(0);
		}
	}

	/*
	 * public IndexFeatureValueOptions(String inputPath, String outputPath, int
	 * numberOfClusters, int numberOfFeatures, int numberOfIterations, double
	 * alpha) { this.inputPath = inputPath; this.outputPath = outputPath;
	 * this.numberOfClusters = numberOfClusters; this.numberOfFeatures =
	 * numberOfFeatures; }
	 */

	public String getInputPath() {
		return inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public String getIndexPath() {
		return indexPath;
	}

	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public int getMinimumCutoffThreshold() {
		return minimumCutoffThreshold;
	}

	public int getMaximumCutoffThreshold() {
		return maximumCutoffThreshold;
	}

	public String toString() {
		return IndexFeatureValueSpark.class.getSimpleName();
	}
}