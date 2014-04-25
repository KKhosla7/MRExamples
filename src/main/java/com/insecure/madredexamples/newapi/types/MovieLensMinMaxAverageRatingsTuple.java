package com.insecure.madredexamples.newapi.types;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Karan Khosla
 */
public class MovieLensMinMaxAverageRatingsTuple implements WritableComparable<MovieLensMinMaxAverageRatingsTuple> {


    private Double minRatings;
    private Double maxRatings;
    private Double averageRatings;


    @Override
    public int compareTo(MovieLensMinMaxAverageRatingsTuple ratingsTuple) {
            return ratingsTuple.minRatings.compareTo(minRatings);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(minRatings);
        dataOutput.writeDouble(maxRatings);
        dataOutput.writeDouble(averageRatings);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        minRatings = dataInput.readDouble();
        maxRatings = dataInput.readDouble();
        averageRatings = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MovieLensMinMaxAverageRatingsTuple that = (MovieLensMinMaxAverageRatingsTuple) o;

        if (!averageRatings.equals(that.averageRatings)) return false;
        if (!maxRatings.equals(that.maxRatings)) return false;
        if (!minRatings.equals(that.minRatings)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = minRatings.hashCode();
        result = 31 * result + maxRatings.hashCode();
        result = 31 * result + averageRatings.hashCode();
        return result;
    }
}