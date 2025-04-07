package com.lxpeak.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import com.lxpeak.mydb.backend.tm.TransactionManagerImpl;

// vm对一个事务的抽象
public class Transaction {
    public long xid;
    // 哪种事务级别，0读已提交，1可重复读
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        // 可重复读级别时使用。
        // 记录在事务T开始时处于active状态的事务（就是还未提交的事务）
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    // 可重复读级别时使用。
    // 记录在事务T开始时处于active状态的事务（就是还未提交的事务）。
    // 如果本事务T1开始时，其他事务T2还是active状态，则忽略事务T2。
    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
