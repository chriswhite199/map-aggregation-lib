package org.apache.hadoop.mapreduce;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Simple Object Pool where all objects can be 'reclaimed'
 */
public class ObjectPool<T> implements Configurable {
    /** Object pool */
    protected ArrayList<T> objectList = new ArrayList<T>();

    /** track index of next object */
    protected int nextIdx = 0;

    /** Object type class */
    protected Class<T> clz;

    /**
     * Factory method to create a new instance
     * 
     * @param clz
     *            Object type class
     * @param conf
     *            Hadoop configuration
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <X> ObjectPool<X> createInstance(Class<X> clz,
            Configuration conf) {
        ObjectPool<X> pool = ReflectionUtils
                .newInstance(ObjectPool.class, conf);
        pool.setClz(clz);

        return pool;
    }

    /** Configuration */
    protected Configuration conf;

    /**
     * Reclaim all objects, making them available again
     */
    public void reclaimAll() {
        nextIdx = 0;
    }

    /**
     * Get an object from the pool
     * 
     * @return
     */
    public T get() {
        if (nextIdx == objectList.size()) {
            T obj = ReflectionUtils.newInstance(clz, conf);
            objectList.add(obj);
            nextIdx++;

            return obj;
        } else {
            return objectList.get(nextIdx++);
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Configure the underlying object class type - this should be only be
     * called during construction in the
     * {@link ObjectPool#createInstance(Class, Configuration)}
     * 
     * @param clz
     */
    protected void setClz(Class<T> clz) {
        this.clz = clz;
    }

    public int getAssignedCount() {
        return nextIdx;
    }
}
