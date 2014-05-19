package com.insecure.com.insecure.hive.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

/**
 * Created by Karan Khosla
 */
public class MaxUDAF extends GenericUDAFEvaluator {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new AggBuffer();
    }

    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        ((AggBuffer) aggregationBuffer).data = null;
    }

    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {

    }


    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        return null;
    }

    @Override
    public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {

    }

    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        return null;
    }

    private static class AggBuffer implements AggregationBuffer {
        public Object data;
    }
}
