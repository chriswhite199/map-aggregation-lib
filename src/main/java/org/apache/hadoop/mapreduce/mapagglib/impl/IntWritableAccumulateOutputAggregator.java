package org.apache.hadoop.mapreduce.mapagglib.impl;

import java.util.Comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.mapagglib.AbstractAccumulateOutputAggregator;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Implementation of a {@link AbstractAccumulateOutputAggregator} which performs
 * {@link IntWritable} summation
 * 
 * @param <K>
 */
public class IntWritableAccumulateOutputAggregator<K> extends
        AbstractAccumulateOutputAggregator<K, IntWritable> {

    public IntWritableAccumulateOutputAggregator(
            Mapper<?, ?, K, IntWritable>.Context context,
            Comparator<K> keyComparator, Class<K> keyClz, int maxBufferSize) {
        super(context, keyComparator, keyClz, IntWritable.class, maxBufferSize);
    }

    @Override
    protected void accumulateValues(K key, IntWritable bufferedValue,
            IntWritable newValue) {
        bufferedValue.set(bufferedValue.get() + newValue.get());
    }
}
