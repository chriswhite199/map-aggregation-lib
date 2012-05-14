package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.util.ReflectionUtils;

/**
 * Abstract partial implementation where the values are buffered in an array
 * list, and flushed when the total number of values (across all keys) reaches
 * some threshold
 * 
 * @param <K>
 * @param <V>
 */
public abstract class AbstractListBackedOutputAggregator<K, V> extends
        AbstractOutputAggregator<K, V> {

    /** buffered output key , values */
    protected Map<K, List<V>> keyedValuesMap;

    /** Maximum number of buffered values before reduce flush */
    protected int maxValuesCount;

    protected ObjectPool<ArrayList> listObjectPool;

    protected AbstractListBackedOutputAggregator(
            Mapper<?, ?, K, V>.Context context, Comparator<K> keyComparator,
            Class<K> keyClz, Class<V> valueClz, int maximumBufferSize) {
        super(context, keyComparator, keyClz, valueClz);

        this.maxValuesCount = maximumBufferSize;

        // get group comparator
        keyedValuesMap = new TreeMap<K, List<V>>(keyComparator);

        listObjectPool = new ObjectPool<ArrayList>();
        listObjectPool.setClz(ArrayList.class);
    }

    /**
     * Reduce the current buffered keyed and values to the output context and
     * clear out the buffer - remember to call this before in your
     * Mapper.cleanup method to ensure everything is flushed to the reducers
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void reduceBuffers() throws IOException, InterruptedException {
        for (Entry<K, List<V>> entry : keyedValuesMap.entrySet()) {
            reduce(entry.getKey(), entry.getValue(), context);

            // clear out list
            entry.getValue().clear();
        }

        // clear out map
        keyedValuesMap.clear();

        // reclaim all objects from the pools
        keyObjectPool.reclaimAll();
        valueObjectPool.reclaimAll();
        listObjectPool.reclaimAll();
    }

    @Override
    public void aggregate(K key, V value) throws IOException,
            InterruptedException {
        // find output stream for key
        List<V> values = keyedValuesMap.get(key);
        if (values == null) {
            // need to get a new list
            values = listObjectPool.get();

            // copy key to use as map-key
            keyedValuesMap.put(
                    ReflectionUtils.copy(conf, key, keyObjectPool.get()),
                    values);
        }

        // copy value to new value and add to list of values for this key
        values.add(ReflectionUtils.copy(conf, value, valueObjectPool.get()));

        // flush if required
        if (valueObjectPool.getAssignedCount() >= maxValuesCount) {
            reduceBuffers();
        }
    }

    /**
     * Perform the aggregation of the given key and associated values. Output
     * should be sent to the context
     * 
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected abstract void reduce(K key, Iterable<V> values,
            Mapper<?, ?, K, V>.Context context) throws IOException,
            InterruptedException;
}
