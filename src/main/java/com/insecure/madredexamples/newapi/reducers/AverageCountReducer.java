package com.insecure.madredexamples.newapi.reducers;

import com.insecure.madredexamples.newapi.types.AverageCountTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Karan Khosla
 */
public class AverageCountReducer extends Reducer<IntWritable, AverageCountTuple, IntWritable, AverageCountTuple> {

private AverageCountTuple result = new AverageCountTuple();

@Override
public void reduce(IntWritable key, Iterable<AverageCountTuple> values,
        Context context) throws IOException, InterruptedException {

        float sum = 0;
        float count = 0;

        // Iterate through all input values for this key
        for (AverageCountTuple val : values) {
        sum += val.getCount() * val.getAverage();
        count += val.getCount();
        }

        result.setCount(count);
        result.setAverage(sum / count);

        context.write(key, result);
        }
}
