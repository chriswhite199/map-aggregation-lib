package org.apache.hadoop.mapreduce.impl;

import java.util.Comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.AbstractAccumulateOutputAggregator;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Implementation of a {@link AbstractAccumulateOutputAggregator} which performs
 * {@link IntWritable} summation
 * 
 * @param <K>
 */
public class IntWritableAccumulateOutputAggregator<K> extends
        AbstractAccumulateOutputAggregator<K, IntWritable> {

    protected IntWritableAccumulateOutputAggregator(
            Mapper<?, ?, K, IntWritable>.Context context,
            Comparator<K> keyComparator, Class<K> keyClz,
            Class<IntWritable> valueClz, int maxBufferSize) {
        super(context, keyComparator, keyClz, valueClz, maxBufferSize);
    }

    @Override
    protected void accumulateValues(K key, IntWritable bufferedValue,
            IntWritable newValue) {
        bufferedValue.set(bufferedValue.get() + newValue.get());
    }
}
