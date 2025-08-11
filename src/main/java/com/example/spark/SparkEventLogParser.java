package com.example.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkEventLogParser {

    public static void main(String[] args) throws IOException {

        String filePath = "D:\\Downloads\\application_1754544985721_0009_2";
        File file = new File(filePath);
        if (!file.exists()) {
            System.out.println("文件不存在: " + filePath);
            System.exit(1);
        }

        ObjectMapper mapper = new ObjectMapper();
        Pattern exitCodePattern = Pattern.compile("Exit code: (\\d+)");

        System.out.printf("%-6s %-12s %-9s %s%n", "JobID", "Status", "ExitCode", "Reason");
        System.out.println("--------------------------------------------------------------------------------");

        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                JsonNode event;
                try {
                    event = mapper.readTree(line.trim());
                } catch (Exception e) {
                    continue; // 忽略无效行
                }

                if ("SparkListenerJobEnd".equals(event.path("Event").asText())) {
                    int jobId = event.path("Job ID").asInt();
                    JsonNode jobResultNode = event.path("Job Result");
                    String status = jobResultNode.path("Result").asText();
                    String reason = null;
                    String exitCodeStr = null;

                    if ("JobFailed".equals(status)) {
                        if (jobResultNode.has("Reason")) {
                            reason = jobResultNode.path("Reason").asText("");
                        } else if (jobResultNode.has("Exception")) {
                            reason = jobResultNode.path("Exception").path("Message").asText("");
                        }
                        Matcher matcher = exitCodePattern.matcher(reason);
                        if (matcher.find()) {
                            exitCodeStr = matcher.group(1);
                        }
                    }

                    System.out.printf("%-6d %-12s %-9s %s%n",
                            jobId,
                            status,
                            exitCodeStr == null ? "None" : exitCodeStr,
                            reason == null ? "" : reason);
                }
            }
        }
    }
}
