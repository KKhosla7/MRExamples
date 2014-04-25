package com.insecure.madredexamples.newapi.types;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Karan Khosla
 */
public class MovieLensRatingsTuple implements Writable {

    private int userId;
    private int itemId;
    private int ratings;
    private long timestamp;
    private String itemName;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(userId);
        dataOutput.writeInt(itemId);
        dataOutput.writeInt(ratings);
        dataOutput.writeLong(timestamp);
        dataOutput.writeUTF(itemName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId = dataInput.readInt();
        itemId = dataInput.readInt();
        ratings = dataInput.readInt();
        timestamp = dataInput.readLong();
        itemName = dataInput.readUTF();
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getItemId() {
        return itemId;
    }

    public void setItemId(int itemId) {
        this.itemId = itemId;
    }

    public int getRatings() {
        return ratings;
    }

    public void setRatings(int ratings) {
        this.ratings = ratings;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    @Override
    public String toString() {
        return "MovieLensRatingsTuple{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", ratings=" + ratings +
                ", timestamp=" + timestamp +
                ", itemName='" + itemName + '\'' +
                '}';
    }
}