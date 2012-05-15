package org.apache.hadoop.mapreduce.mapagglib;

import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class AbstractOutputAggregator<K, V> {
    /** Configuration */
    protected Configuration conf;

    /** Output Key Class */
    protected Class<K> keyClz;

    /** Output Value Class */
    protected Class<V> valueClz;

    /** Value object pool */
    protected ObjectPool<V> valueObjectPool;

    /** Value object pool */
    protected ObjectPool<K> keyObjectPool;

    /** Output context */
    protected Mapper<?, ?, K, V>.Context context;

    /** Key comparator */
    protected Comparator<K> keyComparator;

    /**
     * 
     * @param context
     *            Output context - where map-side reduces are flushed to
     * @param keyComparator
     *            Comparator to determine key grouping
     * @param keyClz
     *            Key Class
     * @param valueClz
     *            Value Class
     */
    protected AbstractOutputAggregator(Mapper<?, ?, K, V>.Context context,
            Comparator<K> keyComparator, Class<K> keyClz, Class<V> valueClz) {
        this.context = context;
        this.conf = context.getConfiguration();
        this.keyComparator = keyComparator;
        this.keyClz = keyClz;
        this.valueClz = valueClz;

        keyObjectPool = new ObjectPool<K>();
        keyObjectPool.setClz(keyClz);

        valueObjectPool = new ObjectPool<V>();
        valueObjectPool.setClz(valueClz);
    }

    /**
     * Aggregate the given key and value, possibly causing a map-side reduction
     * of the currently buffered values
     * 
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void aggregate(K key, V value) throws IOException,
            InterruptedException;
}
