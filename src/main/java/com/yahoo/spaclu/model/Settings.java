package com.yahoo.spaclu.model;

import java.util.regex.Pattern;

public interface Settings {

	// common settings
	public static final String PATH_INDICATOR = "path";
	public static final String INTEGER_INDICATOR = "int";
	public static final String FLOAT_INDICATOR = "float";
	public static final String DOUBLE_INDICATOR = "double";
	public static final String CLASS_INDICATOR = "class";

	public static final String TEMP = "temp";

	public static final char SPACE_CHAR = ' ';
	public static final char UNDER_SCORE_CHAR = '_';
	public static final char TAB_CHAR = '\t';
	public static final char DASH_CHAR = '-';
	public static final char DOT_CHAR = '.';
	public static final char STAR_CHAR = '*';

	public static final String SPACE = " ";
	public static final String UNDER_SCORE = "_";
	public static final String TAB = "\t";
	public static final String DASH = "-";
	public static final String DOT = ".";
	public static final String STAR = "*";
	// public static final char SEPERATOR = '/';

	public static final Pattern TAB_PATTERN = Pattern.compile(TAB);
	public static final Pattern SPACE_PATTERN = Pattern.compile(SPACE);

	public static final String SEPERATOR = "/";

	public static final String HELP_OPTION = "help";

	public static final String INPUT_OPTION = "input";
	public static final String OUTPUT_OPTION = "output";

	public static final String PARTITION_OPTION = "partition";

	/*
	 * public static final double DEFAULT_COUNTER_SCALE = 1e6;
	 * 
	 * public static final String INFERENCE_MODE_OPTION = "test"; public static
	 * final String RANDOM_START_GAMMA_OPTION = "randomstart"; public static
	 * final String MODEL_INDEX = "modelindex"; public static final String
	 * SYMMETRIC_ALPHA = "symmetricalpha";
	 * 
	 * // public static final int DEFAULT_NUMBER_OF_TOPICS = 100;
	 * 
	 * public static final boolean RANDOM_START_GAMMA = false; public static
	 * final boolean LEARNING_MODE = true; public static final boolean RESUME =
	 * false;
	 * 
	 * public static final String TEMP = "temp"; public static final String
	 * GAMMA = "gamma"; public static final String BETA = "beta"; public static
	 * final String ALPHA = "alpha";
	 * 
	 * public static final int MAXIMUM_LOCAL_ITERATION = 100; // public static
	 * final int BURN_IN_SWEEP = 5; public static final double
	 * DEFAULT_GLOBAL_CONVERGE_CRITERIA = 0.000001;
	 * 
	 * public static final double DEFAULT_LOG_ETA = Math.log(1e-12);
	 * 
	 * public static final float DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD =
	 * 0.000001f; public static final int DEFAULT_ALPHA_UPDATE_MAXIMUM_ITERATION
	 * = 1000; public static final int DEFAULT_ALPHA_UPDATE_MAXIMUM_DECAY = 10;
	 * public static final float DEFAULT_ALPHA_UPDATE_DECAY_FACTOR = 0.8f;
	 */

	/**
	 * sub-interface must override this property
	 */
	public static final String PROPERTY_PREFIX = Settings.class.getPackage()
			.getName() + "" + DOT;
}