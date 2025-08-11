package com.example.spark;

import java.io.File;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

public class FileNotFoundJob {
    public static void main(String[] args) throws Exception {
        // test java exception in the last
        SparkConf conf = new SparkConf().setAppName("FileNotFoundJob");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(java.util.Arrays.asList(1, 2, 3, 4, 5));

        List<Integer> collect = rdd.map(x -> x / 1).collect();
        for(Integer num : collect){
            System.out.println(num);
        }

        // 本地读取一个不存在的文件
        Scanner scanner = new Scanner(new File("/nonexistent/path.txt")); // 会抛 FileNotFoundException
        while (scanner.hasNextLine()) {
            System.out.println(scanner.nextLine());
        }

        sc.stop();
    }
}
