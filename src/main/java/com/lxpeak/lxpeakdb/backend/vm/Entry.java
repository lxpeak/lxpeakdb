package com.lxpeak.lxpeakdb.backend.vm;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import com.lxpeak.lxpeakdb.backend.common.SubArray;
import com.lxpeak.lxpeakdb.backend.dm.dataItem.DataItem;
import com.lxpeak.lxpeakdb.backend.utils.Parser;

/**
 * VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [data]
 *
 * XMIN 是创建该条记录（版本）的事务编号，
 * XMAX 则是删除该条记录（版本）的事务编号，
 * DATA 就是这条记录持有的数据。
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN+8;
    private static final int OF_DATA = OF_XMAX+8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        if (dataItem == null) {
            return null;
        }
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public void remove() {
        dataItem.release();
    }

    // 以拷贝的形式返回内容
    public byte[] data() {
        // Q：这里锁住是为什么？
        // A：担心读数据时候有写入操作，所以用了读锁。
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            // Q: 为什么要这么计算？
            // A: (1)首先这一行是开辟了容量为(sa.end - sa.start - OF_DATA)的字节数组，下一行的System.arraycopy才是赋值操作；
            //    (2)其次，通过追踪 SubArray sa = dataItem.data(); 这行代码可以得知：sa的begin是(raw.start+OF_DATA)，end是raw.end，
            //    如果想得到raw（也就是目前代码里的data，也就是实际存储的二进制文件），那么就需要开辟相应的内存空间，也就是后项减前项，
            //    (End-Begin) => raw.end - (raw.start+OF_DATA) => raw.end - raw.start - OF_DATA
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            // 上文得到了空的字节数组，下面代码对空数组进行赋值，然后返回这个字节数组
            System.arraycopy(sa.raw, sa.start+OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    public long getUid() {
        return uid;
    }
}
