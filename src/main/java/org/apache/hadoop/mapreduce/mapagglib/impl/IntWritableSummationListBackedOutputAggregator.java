package org.apache.hadoop.mapreduce.impl;

import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.AbstractListBackedOutputAggregator;
import org.apache.hadoop.mapreduce.AbstractOutputAggregator;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Implementation of the {@link AbstractOutputAggregator} which accumulates a
 * sum of the values (for value type {@link IntWritable})
 * 
 * @param <K>
 */
public class IntWritableSummationListBackedOutputAggregator<K> extends
        AbstractListBackedOutputAggregator<K, IntWritable> {
    protected IntWritable outputValue = new IntWritable();

    public IntWritableSummationListBackedOutputAggregator(
            Mapper<?, ?, K, IntWritable>.Context context,
            Comparator<K> keyComparator, Class<K> keyClz,
            Class<IntWritable> valueClz, int maximumBufferSize) {
        super(context, keyComparator, keyClz, valueClz, maximumBufferSize);
    }

    @Override
    protected void reduce(K key, Iterable<IntWritable> values,
            Mapper<?, ?, K, IntWritable>.Context context) throws IOException,
            InterruptedException {
        outputValue.set(0);

        for (IntWritable value : values) {
            outputValue.set(outputValue.get() + value.get());
        }

        context.write(key, outputValue);
    }

}
