package org.apache.hadoop.mapreduce.impl;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.mock.MockMapContextWrapper;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class IntWritableSummationListBackedOutputAggregatorTest {
    @Test
    public void testAggregatorManualFlush() throws IOException,
            InterruptedException {
        MockMapContextWrapper<?, ?, Text, IntWritable> wrapper = new MockMapContextWrapper(
                null, null, new Configuration());

        Mapper<?, ?, Text, IntWritable>.Context context = wrapper
                .getMockContext();

        IntWritableSummationListBackedOutputAggregator<Text> textAggregator = new IntWritableSummationListBackedOutputAggregator<Text>(
                context, new Text.Comparator(), Text.class, IntWritable.class,
                10000);

        textAggregator.aggregate(new Text("A"), new IntWritable(1));
        textAggregator.aggregate(new Text("A"), new IntWritable(2));
        textAggregator.aggregate(new Text("A"), new IntWritable(4));

        textAggregator.reduceBuffers();

        List<Pair<Text, IntWritable>> outputs = wrapper.getOutputs();

        Assert.assertEquals(1, outputs.size());

        Assert.assertEquals(new Text("A"), outputs.get(0).getFirst());
        Assert.assertEquals(new IntWritable(1 + 2 + 4), outputs.get(0)
                .getSecond());
    }

    @Test
    public void testAggregatorAutoFlush() throws IOException,
            InterruptedException {
        MockMapContextWrapper<?, ?, Text, IntWritable> wrapper = new MockMapContextWrapper(
                null, null, new Configuration());

        Mapper<?, ?, Text, IntWritable>.Context context = wrapper
                .getMockContext();

        IntWritableSummationListBackedOutputAggregator<Text> textAggregator = new IntWritableSummationListBackedOutputAggregator<Text>(
                context, new Text.Comparator(), Text.class, IntWritable.class,
                2);

        textAggregator.aggregate(new Text("A"), new IntWritable(1));
        textAggregator.aggregate(new Text("A"), new IntWritable(2));
        textAggregator.aggregate(new Text("B"), new IntWritable(1));
        textAggregator.aggregate(new Text("A"), new IntWritable(4));
        textAggregator.aggregate(new Text("C"), new IntWritable(1));
        textAggregator.aggregate(new Text("B"), new IntWritable(2));

        textAggregator.reduceBuffers();

        List<Pair<Text, IntWritable>> outputs = wrapper.getOutputs();

        Assert.assertEquals(5, outputs.size());

        Assert.assertEquals(new Text("A"), outputs.get(0).getFirst());
        Assert.assertEquals(new IntWritable(1 + 2), outputs.get(0)
                .getSecond());

        Assert.assertEquals(new Text("A"), outputs.get(1).getFirst());
        Assert.assertEquals(new IntWritable(4), outputs.get(1).getSecond());

        Assert.assertEquals(new Text("B"), outputs.get(2).getFirst());
        Assert.assertEquals(new IntWritable(1), outputs.get(2).getSecond());

        Assert.assertEquals(new Text("B"), outputs.get(3).getFirst());
        Assert.assertEquals(new IntWritable(2), outputs.get(3).getSecond());
        
        Assert.assertEquals(new Text("C"), outputs.get(4).getFirst());
        Assert.assertEquals(new IntWritable(1), outputs.get(4).getSecond());
    }
}
