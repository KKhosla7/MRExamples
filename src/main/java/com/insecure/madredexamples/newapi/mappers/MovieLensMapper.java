package com.insecure.madredexamples.newapi.mappers;

import com.insecure.madredexamples.newapi.types.MovieLensRatingsTuple;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Karan Khosla
 */
public class MovieLensMapper extends Mapper<LongWritable, Text, Text, MovieLensRatingsTuple> {

    IntWritable movieId = new IntWritable();
    Text movieName = new Text();
    Map<String, String> movieIdNameMap = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = StringUtils.split(String.valueOf(value), "\\t");
        if (columns.length < 4) {
            return;
        }
        MovieLensRatingsTuple validMovieRatingsRecord = new MovieLensRatingsTuple();
        validMovieRatingsRecord.setUserId( Integer.parseInt(columns[0]));
        validMovieRatingsRecord.setItemId(Integer.parseInt(columns[1]));
        validMovieRatingsRecord.setRatings(Integer.parseInt(columns[2]));
        validMovieRatingsRecord.setTimestamp(Long.parseLong(columns[3]));
        validMovieRatingsRecord.setItemName( movieIdNameMap.get(columns[1]) );
        movieId.set( Integer.parseInt(columns[1]) );
        movieName.set(movieIdNameMap.get(columns[1]));
        context.write(movieName, validMovieRatingsRecord);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // Distributed cache setup
        Path[] cachedFilesPath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (isCachedFilePathNotEmpty(cachedFilesPath)) {
            for (Path cacheFilePath : cachedFilesPath) {
                System.out.println("File: " + cacheFilePath.getName());
                populateMovieIdMap(cacheFilePath);
            }
        } else {
            System.out.println("No Files on given path.");
        }
    }

    private void populateMovieIdMap(Path cacheFilePath) throws IOException {
        BufferedReader buffer = new BufferedReader(new FileReader(cacheFilePath.getName()));
        try {
            String lineRead;
            while ((lineRead = buffer.readLine()) != null) {
                String[] tokens = lineRead.split("\\|");
                movieIdNameMap.put(tokens[0], tokens[1]);
            }
        } finally {
            safeClose(buffer);
        }
    }

    private void safeClose(Closeable resource) throws IOException {
        resource.close();
    }

    private boolean isCachedFilePathNotEmpty(Path[] emptyCheck) {
        return emptyCheck != null && emptyCheck.length > 0;
    }
}