package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.util.ReflectionUtils;

/**
 * Abstract partial implementation of the {@link AbstractOutputAggregator} which
 * allow in-line aggregation using a single buffered value
 * 
 * @author cswhite
 * 
 * @param <K>
 * @param <V>
 */
public abstract class AbstractAccumulateOutputAggregator<K, V> extends
        AbstractOutputAggregator<K, V> {
    /** Map of key to accumulated value */
    protected Map<K, V> bufferedMap;

    /**
     * Maximum size (number of elements) the buffer can grow to before a flush
     * is triggered
     */
    protected int maxBufferSize;

    /**
     * Constructor
     * 
     * @param context
     *            Output context
     * @param keyComparator
     *            Key comparator
     * @param keyClz
     *            Key Class
     * @param valueClz
     *            Value Class
     */
    protected AbstractAccumulateOutputAggregator(
            Mapper<?, ?, K, V>.Context context, Comparator<K> keyComparator,
            Class<K> keyClz, Class<V> valueClz, int maxBufferSize) {
        super(context, keyComparator, keyClz, valueClz);
        this.maxBufferSize = maxBufferSize;

        bufferedMap = new TreeMap<K, V>(keyComparator);
    }

    @Override
    public void aggregate(K key, V value) throws IOException,
            InterruptedException {
        // get current accumulate value from buffered map
        V currentValue = bufferedMap.get(key);

        // if this is the first value for the key
        if (currentValue == null) {
            // flush if required
            if (bufferedMap.size() >= maxBufferSize) {
                reduceBuffers();
            }

            // create a new value to hold the accumulated value
            currentValue = ReflectionUtils.copy(conf, value,
                    valueObjectPool.get());

            // insert into map for later accumulations
            bufferedMap.put(
                    ReflectionUtils.copy(conf, key, keyObjectPool.get()),
                    currentValue);
        } else {
            // accumulate value into current value
            accumulateValues(key, currentValue, value);
        }
    }

    /**
     * Reduce the current buffered keyed and accumulated value to the output
     * context and clear out the buffer - remember to call this before in your
     * Mapper.cleanup method to ensure everything is flushed to the reducers
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void reduceBuffers() throws IOException, InterruptedException {
        for (Entry<K, V> entry : bufferedMap.entrySet()) {
            context.write(entry.getKey(), entry.getValue());
        }

        // clear out map
        bufferedMap.clear();

        // reclaim all objects from the pool
        keyObjectPool.reclaimAll();
        valueObjectPool.reclaimAll();
    }

    /**
     * Perform accumulation of newValue into the bufferedValue
     * 
     * @param key
     *            The associated key for this newValue, just in case is contains
     *            some state that is to used when accumulating the values
     * @param bufferedValue
     *            Current buffered / accumulated value
     * @param newValue
     *            New value to accumulate into the bufferedValue
     */
    protected abstract void accumulateValues(final K key,
            final V bufferedValue, final V newValue);
}
