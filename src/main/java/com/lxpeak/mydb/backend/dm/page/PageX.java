package com.lxpeak.mydb.backend.dm.page;

import java.util.Arrays;

import com.lxpeak.mydb.backend.dm.pageCache.PageCache;
import com.lxpeak.mydb.backend.utils.Parser;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        // 创建空的page
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        // 对空page设置空闲位置的偏移量
        setFSO(raw, OF_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw, short ofData) {
        // short2Byte()方法会返回两个字节的数组；
        // OF_DATA表示的也是2个字节，表示的是偏移量所占的长度，也就是前者Parser.short2Byte(ofData)的长度。
        // 两者是一个意思
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    // 获取pg的FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw) {
        // 这里就是取出page文件里0-2的字节，这里的0用OF_FREE、2用OF_DATA也可以（或许本该如此🤔）。
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    // 将raw插入pg中，返回插入位置
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        // 这里是对pg的FSO做了修改，并没有修改raw中的FSO，最后会在PageCacheImpl的releaseForCache()方法将pg的数据刷进实际的数据文件中
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    // 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        // 将raw的数据部分赋值给pg
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        // 得到pg的FSO
        short pgFSO = getFSO(pg.getData());
        // Q：为什么用较大的offset
        // A：首先清楚一点，raw是数据源（日志文件）、pg是缓存，需要将raw中的数据更新到pg中，一般来说FSO也应该用raw的FSO，此时对于FSO有两种情况：
        //   （1）pg的FSO>raw的FSO意味着pg后面还有其他的操作记录。通过代码可以知道整个恢复操作会有一个for循环，raw是log中的数据，pg是中间缓存，
        //       也就是说 pg = raw1 - raw2 - raw3 - ...,如果pg的FSO>raw的FSO就说明这个raw不是最后一个需要恢复的日志数据。
        //   （2）
        if(pgFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    // 将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
