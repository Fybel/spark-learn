package com.example.spark;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.List;

public class DivideByZeroJob {
    public static void main(String[] args) {
        // test job error in the middle
        SparkConf conf = new SparkConf().setAppName("DivideByZeroJob");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(java.util.Arrays.asList(1, 2, 3, 4, 5));

        List<Integer> collect = rdd.map(x -> x / 1).collect();
        for(Integer num : collect){
            System.out.println(num);
        }
        JavaRDD<Integer> rdd1 = sc.parallelize(java.util.Arrays.asList(11, 22, 33, 44, 55));

        List<Integer> collect1 = rdd1.map(x -> x / 2).collect();
        for(Integer num : collect1){
            System.out.println(num);
        }

        JavaRDD<Integer> rdd2 = sc.parallelize(java.util.Arrays.asList(12, 23, 34, 45, 56));

        // 会导致 /0 异常
        List<Integer> collect2 = rdd2.map(x -> x / 0).collect();
        for(Integer num : collect2){
            System.out.println(num);
        }

        JavaRDD<Integer> rdd3 = sc.parallelize(java.util.Arrays.asList(13, 24, 35, 46, 57));

        List<Integer> collect3 = rdd3.map(x -> x / 3).collect();
        for(Integer num : collect3){
            System.out.println(num);
        }

        System.out.println("have a try");

        sc.stop();
    }
}
