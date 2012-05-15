package org.apache.hadoop.mapreduce.mapagglib.impl;

import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.mapagglib.AbstractListBackedOutputAggregator;
import org.apache.hadoop.mapreduce.mapagglib.AbstractOutputAggregator;

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
            Comparator<K> keyComparator, Class<K> keyClz, int maximumBufferSize) {
        super(context, keyComparator, keyClz, IntWritable.class,
                maximumBufferSize);
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
