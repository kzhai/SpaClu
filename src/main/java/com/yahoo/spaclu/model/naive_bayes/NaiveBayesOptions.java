package com.yahoo.spaclu.model.naive_bayes;

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
import com.yahoo.spaclu.data.Utility;

public class NaiveBayesOptions {
	final Logger sLogger = LoggerFactory.getLogger(NaiveBayesOptions.class);

	private String inputPath = null;
	private String outputPath = null;
	private int numberOfClusters = 0;
	private int numberOfFeatures = 0;
	private int numberOfIterations = Settings.DEFAULT_ITERATION;
	private int numberOfPartitions = 100;
	private int numberOfSamples = 0;

	private double alpha = Settings.DEFAULT_ALPHA;

	public NaiveBayesOptions(String args[]) {
		Options options = new Options();
		options.addOption(Settings.HELP_OPTION, false, "print the help message");

		options.addOption(OptionBuilder.withArgName(Settings.PATH_INDICATOR)
				.hasArg().withDescription("input file or directory")
				.isRequired().create(Settings.INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName(Settings.PATH_INDICATOR)
				.hasArg().withDescription("output directory").isRequired()
				.create(Settings.OUTPUT_OPTION));

		options.addOption(OptionBuilder.withArgName(Settings.INTEGER_INDICATOR)
				.hasArg().withDescription("number of clusters").isRequired()
				.create(Settings.CLUSTER_OPTION));
		options.addOption(OptionBuilder
				.withArgName(Settings.INTEGER_INDICATOR)
				.hasArg()
				.withDescription(
						"number of iterations (default - "
								+ Settings.DEFAULT_ITERATION + ")")
				.isRequired().create(Settings.ITERATION_OPTION));
		options.addOption(OptionBuilder.withArgName(Settings.INTEGER_INDICATOR)
				.hasArg().withDescription("number of features").isRequired()
				.create(Settings.FEATURE_OPTION));

		options.addOption(OptionBuilder.withArgName(Settings.INTEGER_INDICATOR)
				.hasArg().withDescription("number of partitions")
				.create(Settings.PARTITION_OPTION));
		options.addOption(OptionBuilder.withArgName(Settings.INTEGER_INDICATOR)
				.hasArg().withDescription("number of samples")
				.create(Settings.SAMPLE_OPTION));

		options.addOption(OptionBuilder
				.withArgName(Settings.DOUBLE_INDICATOR)
				.hasArg()
				.withDescription(
						"alpha (default - 1.0/" + Settings.CLUSTER_OPTION + ")")
				.create(Settings.ALPHA_OPTION));

		/*
		 * options.addOption(OptionBuilder.withArgName(Settings.DOUBLE_INDICATOR)
		 * .hasArg() .withDescription("beta (default - 1.0/" +
		 * Settings.CLUSTER_OPTION + ")") .create(Settings.BETA_OPTION));
		 */

		CommandLineParser parser = new GnuParser();
		HelpFormatter formatter = new HelpFormatter();
		try {
			CommandLine line = parser.parse(options, args);

			if (line.hasOption(Settings.HELP_OPTION)) {
				// ToolRunner.printGenericCommandUsage(System.out);
				formatter
						.printHelp(NaiveBayesDriver.class.getName(), options);
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

				outputPath += Utility.getDateTime() + Settings.SEPERATOR;
			}

			if (line.hasOption(Settings.CLUSTER_OPTION)) {
				numberOfClusters = Integer.parseInt(line
						.getOptionValue(Settings.CLUSTER_OPTION));
				Preconditions.checkArgument(numberOfClusters > 0,
						"Illegal settings for " + Settings.CLUSTER_OPTION
								+ " option: must be strictly positive...");
			} else {
				throw new ParseException("Parsing failed due to "
						+ Settings.CLUSTER_OPTION + " not initialized...");
			}
			if (line.hasOption(Settings.ITERATION_OPTION)) {
				numberOfIterations = Integer.parseInt(line
						.getOptionValue(Settings.ITERATION_OPTION));
				Preconditions.checkArgument(numberOfIterations > 0,
						"Illegal settings for " + Settings.ITERATION_OPTION
								+ " option: must be strictly positive...");
			}

			if (line.hasOption(Settings.PARTITION_OPTION)) {
				numberOfPartitions = Integer.parseInt(line
						.getOptionValue(Settings.PARTITION_OPTION));
				Preconditions.checkArgument(numberOfPartitions > 0,
						"Illegal settings for " + Settings.PARTITION_OPTION
								+ " option: must be strictly positive...");
			}
			if (line.hasOption(Settings.SAMPLE_OPTION)) {
				numberOfSamples = Integer.parseInt(line
						.getOptionValue(Settings.SAMPLE_OPTION));
			}

			// TODO: need to relax this contrain in the future
			if (line.hasOption(Settings.FEATURE_OPTION)) {
				numberOfFeatures = Integer.parseInt(line
						.getOptionValue(Settings.FEATURE_OPTION));
			}
			Preconditions.checkArgument(numberOfFeatures > 0,
					"Illegal settings for " + Settings.FEATURE_OPTION
							+ " option: must be strictly positive...");

			if (line.hasOption(Settings.ALPHA_OPTION)) {
				alpha = Double.parseDouble(line
						.getOptionValue(Settings.ALPHA_OPTION));
			}
			/*
			 * Preconditions.checkArgument(alpha > 0, "Illegal settings for " +
			 * Settings.ALPHA_OPTION + " option: must be strictly positive...");
			 */

			/*
			 * if (line.hasOption(Settings.BETA_OPTION)) { beta =
			 * Double.parseDouble(line .getOptionValue(Settings.BETA_OPTION)); }
			 * Preconditions.checkArgument(beta > 0, "Illegal settings for " +
			 * Settings.BETA_OPTION + " option: must be strictly positive...");
			 */
		} catch (ParseException pe) {
			sLogger.error(pe.getMessage());
			formatter.printHelp(NaiveBayesDriver.class.getName(), options);
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

	public NaiveBayesOptions(String inputPath, String outputPath,
			int numberOfClusters, int numberOfFeatures, int numberOfIterations,
			double alpha) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.numberOfClusters = numberOfClusters;
		this.numberOfFeatures = numberOfFeatures;
		this.numberOfIterations = numberOfIterations;
		this.alpha = alpha;
	}

	public String getInputPath() {
		return inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public int getNumberOfClusters() {
		return numberOfClusters;
	}

	public int getNumberOfIterations() {
		return numberOfIterations;
	}

	public double getAlpha() {
		return alpha;
	}

	public int getNumberOfFeatures() {
		return numberOfFeatures;
	}

	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public String toString() {
		return NaiveBayesDriver.class.getSimpleName() + "-K"
				+ numberOfClusters + "-F" + numberOfFeatures + "-I"
				+ numberOfIterations + "-a" + alpha;
	}

	public int getNumberOfSamples() {
		return numberOfSamples;
	}
}