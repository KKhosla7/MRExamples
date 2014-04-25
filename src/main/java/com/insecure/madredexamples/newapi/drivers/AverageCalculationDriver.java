package com.insecure.madredexamples.newapi.drivers;

import com.insecure.madredexamples.newapi.mappers.AverageCountMapper;
import com.insecure.madredexamples.newapi.reducers.AverageCountReducer;
import com.insecure.madredexamples.newapi.types.MinMaxCountTuple;
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
public class AverageCalculationDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AverageCalculationDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        int exitCode = -1;
        try {
             /*
            *
            * The Configuration container for your job configurations. Anything that’s set here is available
            * to map and reduce classes
            *
            * */
            Configuration conf = new Configuration();
            /*
            *
            * Setting up GenericOptionsParser
            *
            * */
            String[] exceptedArguments = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (exceptedArguments.length != 2) {
                System.out.println("Usage: averageCount <input{file|Dir}> <out{file|Dir}>");
                System.exit(2);
            }

            /*
            *
            * The Job class setJarByClass method determines the JAR that contains the class that’s passed-in, which
            * beneath the scenes is copied by Hadoop into the cluster and subsequently set in the Task’s classpath so
            * that your Map/Reduce classes are available to the Task.
            *
            * */
            Job job = new Job(conf, "averageCount");
            job.setJarByClass(MinMaxCountDriver.class);

            // File Input/Output Path
            String[] inputPaths = Arrays.copyOfRange(args, 0, args.length - 1);
            String output = args[args.length - 1];
            Path outputPath = new Path(output);
            FileInputFormat.setInputPaths(job, StringUtils.join(inputPaths, ","));
            FileOutputFormat.setOutputPath(job, outputPath);

            // Delete if the output dir already exists
            outputPath.getFileSystem(conf).delete(outputPath, true);

            // Setting Job Mapper/Reducer Classes
            job.setMapperClass(AverageCountMapper.class);
            job.setReducerClass(AverageCountReducer.class);
            job.setCombinerClass(AverageCountReducer.class);

            // Output Key/Value Pair Type
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MinMaxCountTuple.class);

            // Job Tracker to wait for Job completion
            exitCode = job.waitForCompletion(true) ? 0 : 1;
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return exitCode;
    }

}
