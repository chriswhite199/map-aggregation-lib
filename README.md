map-aggregation-lib
===================

Hadoop - Map-Aggregation-Lib

Hadoop library to assist with map-side aggregation of output values. If you take the Word Count example, 
the mapper outputs <Text, IntWritable> pairs but this is hugely inefficient when the mass bulk of keys are
represented in a small domain set.

To demonstrate this, take the input line "Bob had had a bad day". The standard mapper would output 6 tokens:

    Bob 1
    had 1
    had 1
    a   1
    bad 1
    day 1

The efficiency of this mapper can be improved by using map-side aggregation and performing some of the 
word-counting in the mapper. This reduces the amount of data written by the mapper to disk, and subsequently
data moved through the shuffle and reduce stages:

    Bob 1
    had 2
    a   1
    bad 1
    day 1

While this is a small example, you can imagine a typical corpus of text probably only contains 1,000's of 
unique words. The frequency counts of these words can therefore be maintainted in an in-memory map of <Text,
IntWritable> pairs, and flushed out at the end of the mapper lifecycle (cleanup() method).

This library provides a mechanism for this style of map-aggregation. See the javadocs for the 
AbstractOutputAggregator class, and associated subclasses. Particularly there are two abstract subclasses:

 * org.apache.hadoop.mapreduce.AbstractListBackedOutputAggregator<K, V> - Maintains a list of values for a
   particular key and calls a reduce-like method in the subclassed implementation to reduces the collection
   of values to a single output value
 * org.apache.hadoop.mapreduce.AbstractAccumulateOutputAggregator<K, V> - Maintains a single value for output
   and each call to aggregate delegates to a abstract method which allows the implementation to merge the 
   current and new values together

There are some examples in the impl package which implement IntWritable summation for a particular key for 
both aggregator types
