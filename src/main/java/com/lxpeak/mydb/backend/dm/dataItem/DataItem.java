package com.lxpeak.mydb.backend.dm.dataItem;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import com.lxpeak.mydb.backend.common.SubArray;
import com.lxpeak.mydb.backend.dm.DataManagerImpl;
import com.lxpeak.mydb.backend.dm.page.Page;
import com.lxpeak.mydb.backend.utils.Parser;
import com.lxpeak.mydb.backend.utils.Types;

/*
* DataItem 是 DM 层向上层提供的数据抽象。上层模块通过地址，向 DM 请求到对应的 DataItem，再获取到其中的数据。
* */
public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的offset处解析出DataItem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        short length = (short)(size + DataItemImpl.OF_DATA);
        // 感觉可以直接传值uid，刚通过与运算得到pgno和offset，现在又要或运算得到uid，太浪费了
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
