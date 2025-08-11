package com.example.spark;

import java.io.File;

public class ChangeFileName {
    public static void main(String[] args){
        File directory = new File("E:\\game\\java_learn\\java\\spark-data-skew-demo\\images");
        File[] files = directory.listFiles();
        for (File file : files) {
            String fileName = file.getName();
            String newFileName = fileName.replaceAll("^[^0-9]+", "figure");
            if (!fileName.equals(newFileName)) {
                File newFile = new File(file.getParent(), newFileName);
                boolean renamed = file.renameTo(newFile);
                System.out.println((renamed ? "Renamed: " : "Failed: ") + fileName + " --> " + newFileName);
            }
        }
    }
}
