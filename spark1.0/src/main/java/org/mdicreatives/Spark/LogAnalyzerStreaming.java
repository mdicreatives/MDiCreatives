package org.mdicreatives.Spark;

import org.mdicreatives.Spark.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

// Still working on this code.
public class LogAnalyzerStreaming {
	
	  private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

	  private static class ValueComparator<K, V> implements Comparator<Tuple2<K, V>>, Serializable {
	    private Comparator<V> comparator;

	    public ValueComparator(Comparator<V> comparator) {
	      this.comparator = comparator;
	    }

	    @Override
	    public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
	      return comparator.compare(o1._2(), o2._2());
	    }
	  }

	  // Stats will be computed for the last window length of time.
	  private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
	  // Stats will be computed every slide interval time.
	  private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

	  public static void main(String[] args) {
	    SparkConf conf = new SparkConf().setAppName("Log Analyzer Streaming");
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    JavaStreamingContext jssc = new JavaStreamingContext(sc,
	        SLIDE_INTERVAL);  // This sets the update window to be every 10 seconds.

	    JavaReceiverInputDStream<String> logDataDStream =
	        jssc.socketTextStream("localhost", 9999);

	    // A DStream of Apache Access Logs.
	    JavaDStream<ApacheAccessLog> accessLogDStream =
	        logDataDStream.map(ApacheAccessLog::parseFromLogLine);

} }
