package com.example.spark;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ModifyMDFile {
    public static void main(String[] args){
        List<String> lines = new ArrayList<>();
        File file = new File("E:\\game\\java_learn\\java\\spark-data-skew-demo\\README.md");
        File newFile = new File("E:\\game\\java_learn\\java\\spark-data-skew-demo\\README.md");
        try(BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(newFile))){
            String line;
            while((line = bufferedReader.readLine()) != null){
                Boolean is = line.contains("图片");
                String newLine;
                if(is){
                    newLine = line.replace("图片", "");
                    bufferedWriter.write(newLine+ System.lineSeparator());
                }else {
                    bufferedWriter.write(line + System.lineSeparator());
                }
            }
        }catch (IOException ioException){
            System.out.println(ioException.getMessage());
        }
    }
}
