package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;

public class DynamicExecutorExample {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("DynamicExecutorExample")
                .setMaster("yarn")
                .set("spark.dynamicAllocation.enabled", "true")
                .set("spark.shuffle.service.enabled", "true")
                .set("spark.dynamicAllocation.initialExecutors", "3")
                .set("spark.dynamicAllocation.minExecutors", "1")
                .set("spark.dynamicAllocation.maxExecutors", "6")
                .set("spark.dynamicAllocation.executorIdleTimeout", "10s")
                .set("spark.dynamicAllocation.schedulerBacklogTimeout", "5s");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Step 1: Generate large RDD to trigger shuffle
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 100_0000; i++) {
            data.add(i);
        }

        JavaRDD<Integer> rdd = sc.parallelize(data, 50); // 多个 partition

        // Step 2: Shuffle-heavy operation (groupBy)
        rdd.groupBy(x -> x % 50).count(); // 会触发 shuffle

        System.out.println("First heavy shuffle stage completed.");

        // Step 3: Light workload to trigger executor idle
        Thread.sleep(60000); // 空闲一段时间，触发 executor 被回收

        JavaRDD<Integer> result = rdd.map(x -> x + 1);
        result.count();

        System.out.println("Second light stage completed.");

        sc.stop();
    }
}
