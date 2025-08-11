package com.example.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

public class NullPointerJob {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("NullPointerJob");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.parallelize(java.util.Arrays.asList("a", "b", "c"));

        rdd.map(x -> x.toUpperCase()).collect();

        JavaRDD<String> rdd2 = sc.parallelize(java.util.Arrays.asList("a", null, "c"));

        rdd2.map(x -> x.toUpperCase()).collect(); // null 会导致 NPE

        sc.stop();
    }
}
