package com.insecure.madredexamples.newapi.mappers;

import com.insecure.madredexamples.newapi.types.EmployeeTuple;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
* Created by Karan Khosla
*/
public class EmployeeMapper extends Mapper<LongWritable, Text, EmployeeTuple, Text> {
    EmployeeTuple employeeTuple = new EmployeeTuple();
    IntWritable id = new IntWritable();
    Text name = new Text();
    Text documentId;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        documentId = new Text(filename);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = StringUtils.split(line, ",");
        for (String text : split) {
            id.set(Integer.parseInt(StringUtils.substring(text, 0, text.indexOf(":")).trim()));
            name.set(StringUtils.substring(text, text.indexOf(":") + 1).trim());
            employeeTuple.set(id, name);
            context.write(employeeTuple, documentId);
        }
    }
}
