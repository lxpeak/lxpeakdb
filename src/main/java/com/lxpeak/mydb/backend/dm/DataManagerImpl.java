package com.lxpeak.mydb.backend.dm;

import com.lxpeak.mydb.backend.dm.dataItem.DataItemImpl;
import com.lxpeak.mydb.backend.dm.logger.Logger;
import com.lxpeak.mydb.backend.dm.page.Page;
import com.lxpeak.mydb.backend.dm.page.PageX;
import com.lxpeak.mydb.backend.dm.pageCache.PageCache;
import com.lxpeak.mydb.backend.dm.pageIndex.PageInfo;
import com.lxpeak.mydb.backend.utils.Panic;
import com.lxpeak.mydb.backend.utils.Types;
import com.lxpeak.mydb.common.Error;
import com.lxpeak.mydb.backend.common.AbstractCache;
import com.lxpeak.mydb.backend.dm.dataItem.DataItem;
import com.lxpeak.mydb.backend.dm.page.PageOne;
import com.lxpeak.mydb.backend.dm.pageIndex.PageIndex;
import com.lxpeak.mydb.backend.tm.TransactionManager;

/*
 * DataManager 是 DM 层直接对外提供方法的类，同时，也实现成 DataItem 对象的缓存。
 * DataItem 存储的 key，是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
 *
 * UID结构如下： [pgno (32 bits)] [0 (16 bits)] [offset (16 bits)]
 *
 *
 * */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        // 尝试获取可用页
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            // 首先做日志
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);
            // 再执行插入操作
            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    // 从key中解析出页号，从pageCache中获取到页面，再根据偏移，解析出DataItem
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        // 位掩码操作：(1L << 16) - 1 生成一个低16位全为1的掩码（即0x0000FFFF）。
        //           uid & 0x0000FFFF 提取uid的低16位，赋值给li.offset，表示数据在页内的偏移量。
        short offset = (short)(uid & ((1L << 16) - 1));
        // 等价于uid = uid >>> 32;
        // uid >>>= 32 将uid右移32位，丢弃低32位，保留高32位。
        uid >>>= 32;
        // 位掩码操作：(1L << 32) - 1 生成低32位全为1的掩码（即 0xFFFFFFFF）。
        // uid & 0xFFFFFFFF 提取右移后的低32位，赋值给li.pgno，表示数据所在的页号。
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    // DataItem缓存释放，需要将DataItem写回数据源，由于对文件的读写是以页为单位进行的，只需要将DataItem中的Page对象写回数据源即可（也就是调用release）。
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.initRaw());
        // 如果pgno == 1则继续流程。
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 这里好像冗余了，上面的newPage方法都flush过了，不过中间可能会有更新操作（？），再flush一次也没事
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        // 第1个Page是pageOne，用于保存校验码，而pageNumber从1开始，所以这里i从2开始
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }

            // 当前Page对象的pageNumber；当前Page对象的空闲大小
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            // 注意在使用完 Page 后需要及时 release，否则可能会撑爆缓存。
            pg.release();
        }
    }

}
