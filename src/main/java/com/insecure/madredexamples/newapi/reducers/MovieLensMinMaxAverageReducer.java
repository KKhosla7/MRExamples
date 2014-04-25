package com.insecure.madredexamples.newapi.reducers;

import com.insecure.madredexamples.newapi.types.MovieLensMinMaxAverageRatingsTuple;
import com.insecure.madredexamples.newapi.types.MovieLensRatingsTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Karan Khosla
 */
public class MovieLensMinMaxAverageReducer extends Reducer<Text, MovieLensRatingsTuple, MovieLensMinMaxAverageRatingsTuple, NullWritable> {

    NullWritable noValue =  NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<MovieLensRatingsTuple> values, Context context) throws IOException, InterruptedException {
        Double sumRatings = 0.0;
        Integer counter = 0;
        Integer maxRatings = 0;
        Integer maxRatingsUserId = 0;

        Integer minRatings = 0;
        Integer minRatingsUserId = 0;

        String movieName = "Empty";

        System.out.println("Reducer::values: " + values);
        for (MovieLensRatingsTuple tuple : values) {

        }
    }
}
