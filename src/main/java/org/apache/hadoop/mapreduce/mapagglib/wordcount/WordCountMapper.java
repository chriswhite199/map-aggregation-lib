package org.apache.hadoop.mapreduce.mapagglib.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.mapagglib.impl.IntWritableAccumulateOutputAggregator;

/**
 * Amended word count mapper to utilize the
 * {@link IntWritableAccumulateOutputAggregator}
 */
public class WordCountMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    protected IntWritableAccumulateOutputAggregator<Text> wordFreqMap;

    protected IntWritable one = new IntWritable(1);

    protected Text word = new Text();

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // flush any residual word frequency counts to the output context
        wordFreqMap.reduceBuffers();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());

            // aggregate the word count
            wordFreqMap.aggregate(word, one);
        }
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        // configure the local value aggregator
        wordFreqMap = new IntWritableAccumulateOutputAggregator<Text>(context,
                new Text.Comparator(), Text.class, 10000);
    }
}
