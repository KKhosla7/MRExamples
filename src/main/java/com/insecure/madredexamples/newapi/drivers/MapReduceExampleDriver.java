package com.insecure.madredexamples.newapi.drivers;

import org.apache.hadoop.util.ProgramDriver;

/**
 * Created by Karan Khosla
 */
public class MapReduceExampleDriver {
    public static void main(String[] args) {
        int exitCode = -1; // failure
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("WordCount", WordCountDriver.class, "Usage: WordCount <input{file|Dir}> <out{file|Dir}>" +
                    "\n\t\t\tWordCount Example using the New API.");
            programDriver.addClass("index", InvertedIndexDriver.class, "Usage: index <input{file|Dir}> <out{file|Dir}>" +
                    "\n\t\t\tBuild Inverted Index using the New API.");
            programDriver.addClass("employeeJob", EmployeeJobDriver.class, "Usage: employeeJob <input{file|Dir}> <out{file|Dir}>" +
                    "\n\t\t\tBuild Employee Inverted Index using the New API.");
            programDriver.addClass("minMaxCount", MinMaxCountDriver.class, "Usage: minMaxCount <input{file|Dir}> <out{file|Dir}>" +
                    "\n\t\t\tMin|Max|Count using the New API.");
            programDriver.addClass("averageCount", MinMaxCountDriver.class, "Usage: averageCount <input{file|Dir}> <out{file|Dir}>" +
                    "\n\t\t\tAverage Calculation using the New API.");
            programDriver.addClass("movieLens", MovieLensDriver.class, "Usage: movieLens <input{file|Dir}> <out{file|Dir}>" +
                    "\n\t\t\tMovieLens Use Case using the New API.");
            programDriver.driver(args);
            exitCode = 0; // Success
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }
}