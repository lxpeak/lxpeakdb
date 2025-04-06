package com.lxpeak.mydb.backend.dm;

import com.lxpeak.mydb.backend.dm.dataItem.DataItem;
import com.lxpeak.mydb.backend.dm.logger.Logger;
import com.lxpeak.mydb.backend.dm.page.PageOne;
import com.lxpeak.mydb.backend.dm.pageCache.PageCache;
import com.lxpeak.mydb.backend.tm.TransactionManager;

/*
* DataManager 是 DM 层直接对外提供方法的类，同时，也实现成 DataItem 对象的缓存。
* DataItem 存储的 key，是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
* */
public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /*
    * 从已有文件创建DataManager和从空文件创建DataManager的流程稍有不同，
    * 除了PageCache和Logger的创建方式有所不同以外，从空文件创建首先需要对第一页进行初始化，
    * 而从已有文件创建，则是需要对第一页进行校验，来判断是否需要执行恢复流程。并重新对第一页生成随机字节
    * */
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    // 从已有文件创建，需要对第一页进行校验，来判断是否需要执行恢复流程。并重新对第一页生成随机字节
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        // 设置校验码
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
