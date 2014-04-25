package com.insecure.madredexamples.newapi.types;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Karan Khosla
 */
public class EmployeeTuple implements WritableComparable<EmployeeTuple> {

    private IntWritable id;
    private Text name;

    public EmployeeTuple() {
        set(new IntWritable(), new Text());
    }

    @Override
    public int compareTo(EmployeeTuple employee) {
        return id.compareTo(employee.id);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        name.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        name.readFields(dataInput);
    }

    public void set(IntWritable id, Text name) {
        this.id = id;
        this.name = name;
    }

    public IntWritable getId() {
        return id;
    }

    public Text getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmployeeTuple that = (EmployeeTuple) o;
        return id.equals(that.id) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }


    @Override
    public String toString() {
        return "EmployeeWritable{" +
                "id=" + id +
                ", name=" + name +
                '}';
    }
}