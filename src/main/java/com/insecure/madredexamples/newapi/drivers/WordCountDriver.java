package com.insecure.madredexamples.newapi.drivers;

import com.insecure.madredexamples.newapi.mappers.ValueTokenizeMapper;
import com.insecure.madredexamples.newapi.reducers.ValueLongSumReducer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Karan Khosla
 */
public class WordCountDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        int exitCode = -1;
        try {
            // Job Configuration
            Configuration conf = new Configuration();
            try {
                String[] exceptedArguments = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (exceptedArguments.length != 2) {
                    System.out.println("Usage: WordCount <input{file|Dir}> <out{file|Dir}>");
                    System.exit(2);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            Job job = new Job(conf, "WordCount");
            job.setJarByClass(WordCountDriver.class);

            // File Input/Output Path
            String[] inputPaths = Arrays.copyOfRange(args, 0, args.length - 1);
            String output = args[args.length - 1];
            Path outputPath = new Path(output);
            FileInputFormat.setInputPaths(job, StringUtils.join(inputPaths, ","));
            FileOutputFormat.setOutputPath(job, outputPath);

            // Delete if the output dir already exists
            outputPath.getFileSystem(conf).delete(outputPath, true);

            // Setting Job Mapper/Reducer Classes
            job.setMapperClass(ValueTokenizeMapper.class);
            job.setReducerClass(ValueLongSumReducer.class);
            job.setCombinerClass(ValueLongSumReducer.class);

            // Output Key/Value Pair Type
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            // Job Tracker to wait for Job completion
            exitCode = job.waitForCompletion(true) ? 0 : 1;
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return exitCode;
    }
}