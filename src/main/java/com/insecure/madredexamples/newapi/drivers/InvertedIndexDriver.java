package com.insecure.madredexamples.newapi.drivers;

import com.insecure.madredexamples.newapi.mappers.ValueMap;
import com.insecure.madredexamples.newapi.reducers.ValueReduce;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
public class InvertedIndexDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new InvertedIndexDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        int exitCode = -1;
        try {
            Configuration conf = new Configuration();

            String[] exceptedArguments = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (exceptedArguments.length != 2) {
                System.out.println("Usage: index <input{file|Dir}> <out{file|Dir}>");
                System.exit(2);
            }

            Job job = new Job(conf, "index");
            job.setJarByClass(InvertedIndexDriver.class);

            job.setMapperClass(ValueMap.class);
            job.setReducerClass(ValueReduce.class);


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            String[] inputPaths = Arrays.copyOfRange(args, 0, args.length - 1);
            String output = args[args.length - 1];
            Path outputPath = new Path(output);
            FileInputFormat.setInputPaths(job, StringUtils.join(inputPaths, ", "));
            FileOutputFormat.setOutputPath(job, outputPath);
            outputPath.getFileSystem(conf).delete(outputPath, true);

            exitCode = job.waitForCompletion(true) ? 0 : 1;
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return exitCode;
    }
}
