package com.insecure.madredexamples.newapi.mappers;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
* Created by Karan Khosla
*/
public class ValueMap extends Mapper<LongWritable, Text, Text, Text> {

    Text word = new Text();
    Text documentId = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        documentId = new Text(filename);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] text = StringUtils.split(value.toString(), " ");

        for (String str : text) {
            word.set(str);
            context.write(word, documentId);
        }

    }
}
