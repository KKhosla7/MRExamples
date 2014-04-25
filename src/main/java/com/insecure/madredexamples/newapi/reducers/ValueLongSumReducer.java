package com.insecure.madredexamples.newapi.reducers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
* Created by Karan Khosla
*/
public class ValueLongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0L;
        for (LongWritable value : values)
            sum += value.get();
        context.write(key, new LongWritable(sum));
    }
}
