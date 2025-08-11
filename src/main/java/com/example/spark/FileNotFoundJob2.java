package com.example.spark;

import java.io.File;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

public class FileNotFoundJob2 {
    public static void main(String[] args) throws Exception {
        //spark error in the last
        SparkConf conf = new SparkConf().setAppName("FileNotFoundJob2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(java.util.Arrays.asList(1, 2, 3, 4, 5));

        List<Integer> collect = rdd.map(x -> x / 1).collect();
        for(Integer num : collect){
            System.out.println(num);
        }

        // 尝试读取本地文件（worker节点的本地文件系统）
        // 如果文件不存在，会抛 FileNotFoundException
        String localFilePath = "file:///nonexistent/path.txt";
        JavaRDD<String> lines = sc.textFile(localFilePath);  // Spark 尝试读取不存在的文件

        List<String> collected = lines.collect();
        for (String line : collected) {
            System.out.println(line);
        }

        sc.stop();
    }
}
