# SpaClu

Clustering on Spark.

## Setup Spark

### On local machine

Install and setup Spark on your local machine according to [Building Spark with Maven](http://spark.apache.org/docs/latest/building-with-maven.html).

Set the `$YOUR_SPARK_HOME` to the location (or the directory) where Spark is installed.

### On cluster

Setup Spark and environment variables on the ygrid cluster according to [spark_bundle_1.0](https://git.corp.yahoo.com/hdas/spark_bundle_1.0).

#### ATTENTION

Yahoo has included a Spark distribution with recently cluster upgrade, you may need to configurate Spark according to [Spark On Yarn Product ](http://twiki.corp.yahoo.com/view/Grid/SparkOnYarnProduct).
All the following commands are written in the way using the old settings, which might be different from new settings.

## Build SpaClu

Checkout the SpaClu project from github repo:

	git clone git@git.corp.yahoo.com:kezhai/SpaClu.git

Change to the SpaClu directory:

	cd SpaClu

Build SpaClu jars using Maven with one of the following command:

	mvn package
	mvn clean package

## Running Test Programs

Update the .jar file to the cluster:

	scp target/spaclu-0.1.0-SNAPSHOT-fatjar.jar kezhai@zanium-gw.tan.ygrid.yahoo.com:~/Workspace/

Upload test dataset to the cluster:

	hadoop fs -put input/example/word_count.dat /user/kezhai/input/example/
	hadoop fs -put input/example/logistic_regression.dat /user/kezhai/input/example/

### Local Mode on Single Machine

Run the test JavaSparkPi program on local machine:

	$YOUR_SPARK_HOME/bin/spark-submit \
	--master local[*] \
	--num-executors 3 \
	--executor-memory 2g \
	--driver-memory 3g \
	--class com.yahoo.spaclu.example.JavaSparkPi \
	target/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	100

Run the test JavaWordCount program locally:

	# note that the dataset is on local machine
	
	$YOUR_SPARK_HOME/bin/spark-submit \
	--master local[*] \
	--num-executors 3 \
	--executor-memory 2g \
	--driver-memory 3g \
	--class com.yahoo.spaclu.example.JavaWordCount \
	target/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	input/example/word_count.dat

Run the test JavaLogisticRegression program locally:

	# note that the dataset is on local machine
	
	$YOUR_SPARK_HOME/bin/spark-submit \
	--master local[*] \
	--num-executors 3 \
	--executor-memory 2g \
	--driver-memory 3g \
	--class com.yahoo.spaclu.example.JavaLogisticRegression \
	target/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	input/example/logistic_regression.dat \
	10

Run the NaiveBayesDriver program locally on small test data:

	$YOUR_SPARK_HOME/bin/spark-submit \
	--master local[*] \
	--num-executors 3 \
	--executor-memory 2g \
	--driver-memory 3g \
	--class com.yahoo.spaclu.model.naive_bayes.NaiveBayesDriver \
	target/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	-input input/example/naive_bayes.small.dat \
	-output output/model/naive_bayes.small/ \
	-iteration 10 \
	-cluster 10 \
	-feature 4 

### Local Mode on Cluster

Run the test JavaSparkPi program on the cluster locally:

	spark_run.sh \
	--master local[*] \
	--driver-memory 3g \
	--class com.yahoo.spaclu.example.JavaSparkPi \
	~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	100

Run the test JavaWordCount program on the cluster locally:

	# note that even the program runs in local model, the dataset is actually on HDFS
	
	spark_run.sh \
	--master local[*] \
	--driver-memory 3g \
	--class com.yahoo.spaclu.example.JavaWordCount \
	~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	/user/kezhai/input/example/word_count.dat

Run the test JavaLogisticRegression program on the cluster locally:

	# note that even the program runs in local model, the dataset is actually on HDFS
	
	spark_run.sh \
	--master local[*] \
	--driver-memory 3g \
	--class com.yahoo.spaclu.example.JavaLogisticRegression \
	~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	/user/kezhai/input/example/logistic_regression.dat \
	10

Run the NaiveBayesDriver program on the cluster locally on test data:

	# note that even the program runs in local model, the dataset is actually on HDFS
	
	spark_run.sh \
	--master local[*] \
	--num-executors 3 \
	--executor-memory 2g \
	--driver-memory 3g \
	--class com.yahoo.spaclu.model.naive_bayes.NaiveBayesDriver \
	~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	-input /user/kezhai/input/example/naive_bayes.small.dat \
	-output /user/kezhai/output/model/naive_bayes.small/ \
	-iteration 10 \
	-cluster 10 \
	-feature 4 

### Cluster Mode

Run the test JavaSparkPi program on the cluster:

	spark_run.sh \
	--master yarn-cluster \
	--queue adhoc \
	--num-executors 3 \
	--executor-memory 2g \
	--driver-memory 3g \
	--class com.yahoo.spaclu.example.JavaSparkPi \
	~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	100

Run the test JavaWordCount program on the cluster:

	spark_run.sh \
	--master yarn-cluster \
	--queue adhoc \
	--num-executors 3 \
	--executor-memory 2g \
    --driver-memory 3g \
	--class com.yahoo.spaclu.example.JavaWordCount \
	~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	/user/kezhai/input/example/word_count.dat

Run the test JavaLogisticRegression program on the cluster:

	spark_run.sh \
	--master yarn-cluster \
	--queue adhoc \
	--num-executors 3 \
	--executor-memory 2g \
    --driver-memory 3g \
	--class com.yahoo.spaclu.example.JavaLogisticRegression \
	~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	/user/kezhai/input/example/logistic_regression.dat \
	10
	
Run the NaiveBayesDriver program on the cluster on test data:

	spark_run.sh \
	--master yarn-cluster \
	--queue adhoc \
	--num-executors 3 \
	--executor-memory 2g \
    --driver-memory 3g \
	--class com.yahoo.spaclu.model.naive_bayes.NaiveBayesDriver \
	~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	-input /user/kezhai/input/example/naive_bayes.small.dat \
	-output /user/kezhai/output/model/naive_bayes.small/ \
	-iteration 10 \
	-cluster 10 \
	-feature 4 

To find the application ID of the job, you could find it on the WebApp, in the console output, from the logs, or even using the following command:

    APPLICATION_ID="$(yarn application -list | grep ${USER} | sort | tail -n 1 | cut -f 1)"

The application ID is in the format of `application_<#>_<#>`, where `<#>` is all digits. 

To kill the job that is already running:

	yarn application -kill ${APPLICATION_ID}

To save the logs into a file for futher queries:

    yarn logs -applicationId ${APPLICATION_ID} > logs.txt
    
Note that the output from individual slave machine is not displayed on the WebApp, but will be displayed here in the log. 

## Clustering on Spark

### Index 1 Month of Bill Mark Time Data

Index 1 month of Bill Mark Time data.

	# copy the data across different hdfs, otherwise it would have exceptions and erros in file system security, authorization and I/O pro
	
	hadoop fs -cp hdfs://tiberiumtan-nn1.tan.ygrid.yahoo.com:8020/user/meizhu/LoginScoresBillMarkTimeNew /user/kezhai/

`index` option specifies the information of the feature, of the format of `feature_name \t raw (0) or derived (1) feature \t feature_family`.
Note that `max_cutoff` and `min_cutoff` are optional parameters, which define the maximum and minimum cutoff threshold on the number of possible values for a feature.
Any feature with number of possible values that is larger than `max_cutoff` or smaller than `min_cutoff` will be discarded.

	spark_run.sh \
	--master yarn-cluster \
	--queue adhoc \
	--num-executors 20 \
	--executor-memory 8g \
	--driver-memory 4g \
	--class com.yahoo.spaclu.data.index.IndexFeatureValueSpark \
	~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	-input /user/kezhai/LoginScoresBillMarkTime/201405* \
	-output /user/kezhai/input/index-201405 \
	-index /user/kezhai/LoginScoresBillMarkTime/score-vars-sub-families-bill-time.txt \
	-max_cutoff 1000 \
	-min_cutoff 2
	
Set the `-partition 100` to an even larger value to increase the number of partitions, and reduce the load of each mapper.
	
On the output folder, the program produces three directories and files.

	[kezhai@gwta6008 ~]$ hadoop fs -ls /user/kezhai/input/index
	Found 3 items
	drwx------   - kezhai users          0 2014-07-18 00:02 /user/kezhai/input/index-201405/data
	drwx------   - kezhai users          0 2014-07-18 00:01 /user/kezhai/input/index-201405/feature
	-rw-------   3 kezhai users        503 2014-07-18 00:01 /user/kezhai/input/index-201405/feature.dat

`data` directory contains all the indexed data.
`feature` directory contains all the indexed feature.
`feature.dat` file contains the information about the remaining feature (features that are not discarded during indexing).

The number of remaining features can be found using following command, which would be potentially useful for some clustering models.

	[kezhai@gwta6008 ~]$ hadoop fs -cat /user/kezhai/input/index-201405/feature.dat | wc -l
	23

### Index 3 Months of Bill Mark Time Data

Index the Bill Mark Time data using the hadoop mapreduce indexing tool, since spark index tools may run into memory error.

	yarn \
	jar ~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	com.yahoo.spaclu.data.index.IndexFeatureValueHadoop \
	-Dmapred.job.queue.name=adhoc \
	-input /user/kezhai/LoginScoresBillMarkTime/2014* \
	-output /user/kezhai/input/index-2014 \
	-index /user/kezhai/LoginScoresBillMarkTime/score-vars-sub-families-bill-time.txt \
	-max_cutoff 1000 \
	-min_cutoff 2 \
	-partition 100

Use the following options to set the maximize size of a split from the mapper to avoid out of namespace error if necessary

	-Dmapred.max.split.size=1000000000

### Naive Bayes Clustering

	# note that it requires to pass in the number of features in the dataset.
	
	spark_run.sh \
	--master yarn-cluster \
	--queue adhoc \
	--num-executors 100 \
	--executor-memory 8g \
	--driver-memory 6g \
	--class com.yahoo.spaclu.model.naive_bayes.NaiveBayesDriver \
	~/Workspace/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	-input /user/kezhai/input/index-201405/data \
	-output /user/kezhai/output/model/naive_bayes \
	-iteration 10 \
	-cluster 100 \
	-feature 23
	
## Extract Curve Ball Dataset

Extract the Curve Ball data.

	spark_run.sh \
	--master yarn-cluster \
	--queue adhoc \
	--num-executors 100 \
	--executor-memory 8g \
	--driver-memory 6g \
	--class com.yahoo.spaclu.data.extract.ExtractFeatureSpark \
	target/spaclu-0.1.0-SNAPSHOT-fatjar.jar \
	-input /user/prasadch/curveball_impression_stats/ \
	-output /user/kezhai/input/extract-201407 \
	-partition 100
	
## Running Mr. LDA on Curve Ball Data

	yarn \
	jar ~/Workspace/mrlda-0.9.0-SNAPSHOT-fatjar.jar \
	cc.mrlda.ParseCorpus \
	-Dmapred.job.queue.name=adhoc \
	-Dmapred.child.java.opts=-Xmx1024M \
	-input /user/kezhai/input/extract-201407/document \
	-output /user/kezhai/input/parse-201407
