package com.yahoo.spaclu.data;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class Utility {
	public final static int MB = 1024 * 1024;
	public static final Calendar calendar = Calendar.getInstance();
	public static final DateFormat dateFormat = new SimpleDateFormat(
			"yyMMdd-HHmmss-SS");

	public static void printJavaPairRDD(JavaPairRDD<?, ?> javaPairRDD) {
		Iterator<?> iter = javaPairRDD.collect().iterator();
		Tuple2<?, ?> temp;
		while (iter.hasNext()) {
			temp = (Tuple2<?, ?>) iter.next();

			System.out.println(temp._1 + "\t" + temp._2);
		}
	}

	public static void printJavaRDD(JavaRDD<?> javaRDD) {
		Iterator<?> iter = javaRDD.collect().iterator();
		Object temp;
		while (iter.hasNext()) {
			temp = iter.next();
			System.out.println(temp);
		}
	}

	public static void exportAlpha(SequenceFile.Writer sequenceFileWriter,
			double[] alpha) throws IOException {
		IntWritable intWritable = new IntWritable();
		DoubleWritable doubleWritable = new DoubleWritable();
		for (int i = 0; i < alpha.length; i++) {
			doubleWritable.set(alpha[i]);
			intWritable.set(i + 1);
			sequenceFileWriter.append(intWritable, doubleWritable);
		}
	}

	public static boolean writeToHDFS(Object object, String fileName)
			throws IOException {
		// Create a default hadoop configuration
		Configuration conf = new Configuration();
		// Parse created config to the HDFS
		FileSystem fs = FileSystem.get(conf);
		// Specifies a new file in HDFS.
		Path filenamePath = new Path(fileName);
		try {
			// if the file already exists delete it.
			if (fs.exists(filenamePath)) {
				throw new IOException();
				// remove the file
				// fs.delete(filenamePath, true);
			}
			// FSOutputStream to write the inputmsg into the HDFS file
			FSDataOutputStream fos = fs.create(filenamePath);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(object);
			fos.close();
			oos.close();
			return true;
		} catch (IOException ioe) {
			System.err
					.println("IOException during operation " + ioe.toString());
			return false;
		}
	}

	public static String getDateTime() {
		return dateFormat.format(calendar.getTime());
	}

	public static void garbageCollection(String garbageCollectionInfo) {
		System.out.println(garbageCollectionInfo);
		garbageCollection();
	}

	public static void garbageCollection() {
		Runtime.getRuntime().runFinalization();
		System.out.println("Memory profile before garbage collection (MB): "
				+ getMemoryInfo());

		Runtime.getRuntime().gc();
		Runtime.getRuntime().runFinalization();
		System.out.println("Memory profile after garbage collection (MB): "
				+ getMemoryInfo());
	}

	public static String getMemoryInfo() {
		return "total - "
				+ Runtime.getRuntime().totalMemory()
				/ MB
				+ ", free - "
				+ Runtime.getRuntime().freeMemory()
				/ MB
				+ ", used - "
				+ (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
						.freeMemory()) / MB;
	}

	public static void main(String[] args) {
		int[] a = new int[10];
		System.out.println(a.getClass().isArray());

		Utility[] b = new Utility[10];
		System.out.println(b.getClass().isArray());
	}
}