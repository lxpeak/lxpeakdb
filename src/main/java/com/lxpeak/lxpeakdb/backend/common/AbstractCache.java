package com.lxpeak.lxpeakdb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.lxpeak.lxpeakdb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 *
 * 这里用的缓存就是一个用来保存数据库的临时信息的数据结构，为了能控制这个缓存的大小，所以设定了最大缓存数，由于有最大缓存数所以需要有淘汰策略。
 */
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();
            if(getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if(cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 尝试获取该资源
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count ++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            // 缓存没有数据就走到这里
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            // Q：为什么count--
            // A：注意这是catch中的操作，表示执行上面方法出错了，所以撤销了这个方法之前的所有操作，如果没有出错的话不会执行这里的语句
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        // 因为获取到了数据，所以删掉getting中正在等待的key，并将获取的数据放进实际缓存cache中
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        // Q：这里的1是对的吗？不应该是references.get(key) - 1吗？
        // A：是对的，这里是第一次将key放进references中，如果以后还需要访问这个key，会在上面的while循环里返回
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key)-1;
            if(ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count --;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
