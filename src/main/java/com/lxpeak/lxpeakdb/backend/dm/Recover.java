package com.lxpeak.lxpeakdb.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.primitives.Bytes;

import com.lxpeak.lxpeakdb.backend.common.SubArray;
import com.lxpeak.lxpeakdb.backend.dm.logger.Logger;
import com.lxpeak.lxpeakdb.backend.dm.page.Page;
import com.lxpeak.lxpeakdb.backend.dm.page.PageX;
import com.lxpeak.lxpeakdb.backend.dm.pageCache.PageCache;
import com.lxpeak.lxpeakdb.backend.utils.Panic;
import com.lxpeak.lxpeakdb.backend.dm.dataItem.DataItem;
import com.lxpeak.lxpeakdb.backend.tm.TransactionManager;
import com.lxpeak.lxpeakdb.backend.utils.Parser;


/*
* updateLog:
* [LogType] [XID] [UID] [OldRaw] [NewRaw]
* insertLog:
* [LogType] [XID] [Pgno] [Offset] [Raw]
* -----------------------------------------------------
* 日志恢复策略：
* 1、重做所有崩溃时已完成（committed 或 aborted）的事务
* 2、撤销所有崩溃时未完成（active）的事务
* */
public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    // 重做所有已完成事务，撤销所有未完成事务
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgno = 0;

        // 找到最大的pgno
        while(true) {
            // 得到一条日志记录
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                // 就为了得到pgno，然后和maxPgno比较
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                // 就为了得到pgno，然后和maxPgno比较，不过细节比插入要复杂一点
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        // maxPgno是最后一个pg，所以文件截取到maxPgno，再往后的都不要了
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    // 重做所有崩溃时已完成（committed 或 aborted）的事务
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // log文件前四字节是校验码，所以直接跳过
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    // 撤销所有崩溃时未完成（active）的事务
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // 更新日志格式如下：
    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        // 从UID提取偏移量
        // 位掩码操作：(1L << 16) - 1 生成一个低16位全为1的掩码（即0x0000FFFF）。
        //           uid & 0x0000FFFF 提取uid的低16位，赋值给li.offset，表示数据在页内的偏移量。
        li.offset = (short)(uid & ((1L << 16) - 1));

        // 等价于uid = uid >>> 32;
        // uid >>>= 32 将uid右移32位，丢弃低32位，保留高32位。
        uid >>>= 32;
        // 位掩码操作：(1L << 32) - 1 生成低32位全为1的掩码（即 0xFFFFFFFF）。
        // uid & 0xFFFFFFFF 提取右移后的低32位，赋值给li.pgno，表示数据所在的页号。
        li.pgno = (int)(uid & ((1L << 32) - 1));

        // length是oldRaw和newRaw各占一半的长度。
        // log.length - OF_UPDATE_RAW 表示日志剩余部分的长度。除以2是因为oldRaw和newRaw长度相等。
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            // 注意：两个判断的区别在这，重做(REDO)用的是新的数据，撤销(UNDO)用的旧的数据
            // 1、撤销(UNDO)意味着之前的更新操作不做了，也就是说没有新的修改数据，只有旧的修改数据，所以用旧数据oldRaw
            // 2、重做(REDO)就是重新再执行一次更新操作，对数据库的数据进行修改，所以肯定是用新数据newRaw来更新。
            // 3、旧数据只存在于log文件里，这里要更新的是Page，也就是真正存数据的数据页，
            //    也就是说这段代码的流程是：从log文件中得到某个数据（可能是旧数据，也可能是新数据），然后放到page对象里对真正的数据库数据进行更新，实现日志的恢复。
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            // 根据pgno从缓存中取出Page对象
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            // Q：更新操作是直接将对应内容替换，此时会有个问题，如果newRaw和oldRaw长度不同，page中后面的数据不就会受影响吗？
            // A：不会，因为newRaw和oldRaw在一开始就是相同长度的。参考上面的parseUpdateLog()方法，newRaw和oldRaw平分日志的剩余部分。
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    // 插入日志格式如下：
    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    // 从日志记录中得到xid（事务id）、pgno、offset和raw（保存的数据）
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            // 根据pgno从缓存中得到对应的Page对象
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            // 撤销所有崩溃时未完成（active）的事务
            if(flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            // 重做所有崩溃时已完成（committed 或 aborted）的事务
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}
