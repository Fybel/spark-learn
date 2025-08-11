package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataSkewSQLExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataSkewSQLExample");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        List<Row> rows = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 1_000_000; i++) {
            String key = random.nextDouble() < 0.9 ? "hot_key" : "key_" + random.nextInt(1000);
            rows.add(RowFactory.create(key, 1));
        }

        StructType schema = new StructType()
                .add("key", "string")
                .add("value", "int");

        Dataset<Row> df = spark.createDataFrame(rows, schema);

        long start1 = System.currentTimeMillis();
        df.groupBy("key").sum("value").collect();
        long end1 = System.currentTimeMillis();
        System.out.println("original groupByKey time usage" + (end1 - start1) + " ms");

        int prefixNum = 10;
        Dataset<Row> dfWithPrefix = df.withColumn("key",
                functions.when(df.col("key").equalTo("hot_key"),
                        functions.concat_ws("_", functions.lit((int) (Math.random() * prefixNum)), df.col("key")))
                        .otherwise(df.col("key"))
        );

        Dataset<Row> partialAgg = dfWithPrefix.groupBy("key").sum("value");
        Dataset<Row> finalAgg = partialAgg.withColumn("key",
                functions.when(partialAgg.col("key").contains("_"),
                        functions.split(partialAgg.col("key"), "_").getItem(1))
                        .otherwise(partialAgg.col("key"))
        ).groupBy("key").sum("sum(value)");

        long start2 = System.currentTimeMillis();
        finalAgg.collect();
        long end2 = System.currentTimeMillis();

        System.out.println("after optimized groupByKey usage: " + (end2 - start2) + " ms");

        sc.stop();
        spark.stop();
    }
}