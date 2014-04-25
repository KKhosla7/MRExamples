package com.insecure.madredexamples.newapi.mappers;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
* Created by Karan Khosla
*/
public class ValueTokenizeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text word = new Text();
    LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString().toLowerCase();
        for (String str : StringUtils.split(text, " ")) {
            word.set(str);
            context.write(word, one);
        }
    }
}
