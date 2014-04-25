package com.insecure.madredexamples.newapi.reducers;

import com.insecure.madredexamples.newapi.types.EmployeeTuple;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
* Created by Karan Khosla
*/
public class EmployeeReducer extends Reducer<EmployeeTuple, Text, EmployeeTuple, Text> {
    Text docIds = new Text();

    @Override
    protected void reduce(EmployeeTuple key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Text> uniqueDocIdList = new ArrayList<>();

        for (Text docId : values) {
            uniqueDocIdList.add(new Text(docId));
        }
        docIds.set(StringUtils.join(uniqueDocIdList, ", "));
        context.write(key, docIds);
    }

}
