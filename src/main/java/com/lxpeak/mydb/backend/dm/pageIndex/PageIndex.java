package com.lxpeak.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.lxpeak.mydb.backend.dm.pageCache.PageCache;

/*
* 页面索引，缓存了每一页的空闲空间。用于在上层模块进行插入操作时，能够快速找到一个合适空间的页面，而无需从磁盘或者缓存中检查每一个页面的信息。
*
* 将一页的空间划分成了 40 个区间。在启动时，就会遍历所有的页面信息，获取页面的空闲空间，安排到这 40 个区间中。
* insert在请求一个页时，会首先将所需的空间向上取整，映射到某一个区间，随后取出这个区间的任何一页，都可以满足需求。
* */
public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    // 一个Page页面的区块大小是THRESHOLD
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    // freeSpace是该pgno对应的空闲大小
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            // 一个Page页面的区块大小是THRESHOLD，所以把每个Page的空闲大小按照THRESHOLD分割。（当然问题是会有碎片空间）
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    // 从PageIndex中获取页面也很简单，算出区间号，直接取即可
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 1、缺陷：如果number是0的话，意味着freeSpace不满足THRESHOLD，也就是不够一个区块的大小，所以是不能用的（其实就是要从number=1开始），会浪费掉所有number=0的Page。
            // 2、Q：如果spaceSize是一个比整个页面PAGE_SIZE还大的值怎么办？
            //    A：在调用select()方法前的insert()方法中已经有了判断，如if(raw.length > PageX.MAX_FREE_SPACE)，会先判断这个长度是否大于页面的最大剩余空间，
            //       因为MAX_FREE_SPACE是小于页面最大值PAGE_SIZE的，所以计算出来的number是小于INTERVALS_NO的。
            int number = spaceSize / THRESHOLD;
            // 如果没有超过最大区块数40的话，那就对spaceSize向上取整，也就是需要number个区块才能保存下的话，就用number+1个区块来保存。
            if(number < INTERVALS_NO) number ++;
            // 上面两句可以优化为下面这一句
            // int number = (spaceSize + THRESHOLD - 1) / THRESHOLD;
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {
                    number ++;
                    continue;
                }
                // 可以注意到，被选择的页会直接从PageIndex中移除，这意味着同一个页面是不允许并发写的。
                // 在上层模块使用完这个页面后，需要将其重新插入PageIndex，也就是调用add方法
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
