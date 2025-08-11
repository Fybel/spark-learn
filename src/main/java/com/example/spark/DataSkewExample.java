package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataSkewExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataSkewExample")
                .set("spark.driver.memory", "800m");
        JavaSparkContext sc = new JavaSparkContext(conf);


        List<Tuple2<String, Integer>> data = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 1_000_000; i++) {
            if (random.nextDouble() < 0.9) {
                data.add(new Tuple2<>("hot_key", 1));
            } else {
                data.add(new Tuple2<>("key_" + random.nextInt(1000), 1));
            }
        }

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data, 4);

        long start = System.currentTimeMillis();
        JavaPairRDD<String, Integer> result = rdd.reduceByKey(Integer::sum);
        result.collect();
        long end = System.currentTimeMillis();

        System.out.println("original reduceByKey time usage: " + (end - start) + " ms");
        sc.stop();
    }
}