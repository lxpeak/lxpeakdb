package com.lxpeak.mydb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.lxpeak.mydb.backend.dm.pageCache.PageCache;

public class PageImpl implements Page {
    // 页号从1开始
    private int pageNumber;
    private byte[] data;
    // 脏页面指内存中被修改但还没有刷回本地的页面
    private boolean dirty;
    private Lock lock;
    
    private PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void release() {
        pc.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }

}
