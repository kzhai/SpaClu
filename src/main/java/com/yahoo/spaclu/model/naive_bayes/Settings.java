package com.yahoo.spaclu.model.naive_bayes;


public interface Settings extends com.yahoo.spaclu.model.Settings {

	// public static final String INDEX_OPTION = "index";

	public static final String ALPHA_OPTION = "alpha";
	public static final String BETA_OPTION = "beta";

	// public static final String DEFAULT_QUEUE_NAME = "default";

	public static final String CLUSTER_OPTION = "cluster";
	public static final String FEATURE_OPTION = "feature";
	public static final String ITERATION_OPTION = "iteration";
	public static final String SAMPLE_OPTION = "sample";

	public static final int DEFAULT_ITERATION = 50;
	public static final double DEFAULT_ALPHA = -1;

	/**
	 * sub-interface must override this property
	 */
	public static final String PROPERTY_PREFIX = Settings.class.getPackage()
			.getName() + "" + DOT;
}