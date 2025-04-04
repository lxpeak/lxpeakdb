package com.lxpeak.mydb.backend.dm.page;

public interface Page {
    boolean isDirty();
    int getPageNumber();
    byte[] getData();

    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
}
