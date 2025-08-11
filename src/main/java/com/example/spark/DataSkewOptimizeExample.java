package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataSkewOptimizeExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataSkewOptimizeExample");
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

        int prefixNum = 10;
        JavaPairRDD<String, Integer> rddWithPrefix = rdd.mapToPair(t -> {
            if ("hot_key".equals(t._1)) {
                int prefix = random.nextInt(prefixNum);
                return new Tuple2<>(prefix + "_" + t._1, t._2);
            }
            return t;
        });

        long start = System.currentTimeMillis();
        JavaPairRDD<String, Integer> partialAgg = rddWithPrefix.reduceByKey(Integer::sum);

        JavaPairRDD<String, Integer> finalResult = partialAgg
                .mapToPair(t -> new Tuple2<>(t._1.contains("_") ? t._1.split("_", 2)[1] : t._1, t._2))
                .reduceByKey(Integer::sum);

        finalResult.collect();
        long end = System.currentTimeMillis();

        System.out.println("after optimized reduceByKey usage: " + (end - start) + " ms");
        sc.stop();
    }
}