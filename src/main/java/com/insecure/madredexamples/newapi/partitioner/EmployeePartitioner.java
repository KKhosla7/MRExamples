package com.insecure.madredexamples.newapi.partitioner;

import com.insecure.madredexamples.newapi.types.EmployeeTuple;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Karan Khosla
 */
public class EmployeePartitioner extends Partitioner<EmployeeTuple, Writable> {

    @Override
    public int getPartition(EmployeeTuple key, Writable value, int numPartitions) {
        return key.getId().hashCode() % numPartitions;
    }
}