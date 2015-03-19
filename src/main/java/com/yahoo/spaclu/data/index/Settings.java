package com.yahoo.spaclu.data.index;

public interface Settings extends com.yahoo.spaclu.model.Settings {
	public static final String INDEX_INDICATOR = "index";

	public static final String MAX_CUTOFF_OPTION = "max_cutoff";
	public static final String MIN_CUTOFF_OPTION = "min_cutoff";

	public static final String MAPPER_OPTION = "mapper";
	public static final String REDUCER_OPTION = "reducer";
	
	public static final String FEATURE_INDICATOR = "feature";

	public static final int DEFAULT_NUMBER_OF_MAPPERS = 100;
	public static final int DEFAULT_NUMBER_OF_REDUCERS = 50;

	public static final String PROPERTY_PREFIX = Settings.class.getPackage()
			.getName() + "" + DOT;
}