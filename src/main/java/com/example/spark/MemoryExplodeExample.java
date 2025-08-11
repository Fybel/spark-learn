package com.example.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.List;

public class MemoryExplodeExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("MemoryExplodeExample");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建简单的 RDD
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(i);
        }

        JavaRDD<Integer> rdd = sc.parallelize(data, 2);

        // 模拟内存爆炸（每个 partition 占用大量内存）
        rdd.mapPartitions(iterator -> {
            List<Integer> results = new ArrayList<>();
            List<byte[]> memoryHog = new ArrayList<>();

            // 每次申请 50MB，申请 20 次就已经 1GB+
            for (int i = 0; i < 20; i++) {
                byte[] block = new byte[50 * 1024 * 1024]; // 50MB
                memoryHog.add(block);
                System.out.println("Allocated " + ((i + 1) * 50) + "MB in partition.");
                Thread.sleep(500); // 放慢一点，便于观察
            }

            while (iterator.hasNext()) {
                results.add(iterator.next());
            }

            return results.iterator();
        }).collect();

        sc.stop();
    }
}

