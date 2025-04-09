package com.lxpeak.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.lxpeak.mydb.backend.dm.DataManager;
import com.lxpeak.mydb.backend.utils.Panic;
import com.lxpeak.mydb.common.Error;
import com.lxpeak.mydb.backend.common.AbstractCache;
import com.lxpeak.mydb.backend.tm.TransactionManager;
import com.lxpeak.mydb.backend.tm.TransactionManagerImpl;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    // read() 方法读取一个 entry，注意判断下可见性即可。
    // 读取真正的数据，以字节数组形式返回
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            // 这里的异常是返回的本对象中getForCache()抛出的异常
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                // 获得保存真正数据的字节数组
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    // insert() 则是将数据包裹成 Entry，无脑交给 DM 插入即可。
    // 返回uid
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    // 主要是前置的三件事：一是可见性判断，二是获取资源的锁，三是版本跳跃判断。删除的操作只有一个设置 XMAX。
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            //可见性判断
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Semaphore l = null;
            try {
                // add方法里会检测是否死锁，并返回Lock对象
                l = lt.add(xid, uid);
            } catch(Exception e) {
                t.err = Error.ConcurrentUpdateException;
                // Q: 为什么先执行internAbort然后再执行t.autoAborted = true？
                // A: 为了防止internAbort方法内部的提前返回，必须确保在调用internAbort方法时autoAborted尚未被设置，从而确保回滚操作得以完整执行。
                //    如果先设置t.autoAborted = true，internAbort方法会直接跳过释放锁（lt.remove(xid)）和更新事务状态（tm.abort(xid)）的关键逻辑。
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            // 如果 l = lt.add(xid, uid);中的l非空，代表获得了锁，也就是UID正被某个UID持有(u2x中存在该UID)，
            // 所以这里会进入if方法里，然后阻塞在l.lock()这里。（理论上应该如此，之前这里用的是ReentrantLock，是可重入锁，所以不会锁住）
            if(l != null) {
                // 阻塞在这一步
                l.acquire();
                l.release();
            }

            // Q：为什么如果Xmax是xid就返回false?
            // A：方法最后会执行一次entry.setXmax(xid)，这里如果相等了，说明已经删除了，不用再次删除（设置XMAX）
            if(entry.getXmax() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    /*
    * begin()开启一个事务，并初始化事务的结构，将其存放在activeTransaction中，用于检查和快照使用。
    * */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    /*
    * commit()方法提交一个事务，主要就是free掉相关的结构，并且释放持有的锁，并修改TM状态。
    * */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        // 释放所有它持有的锁
        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /*
    * abort 事务的方法则有两种，手动和自动。手动指的是调用 abort() 方法，而自动，则是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚：
    * */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        // 如果是手动回滚，则从事务列表中移除
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        // Q：先后有两次判断，一次是autoAborted，一次是t.autoAborted，
        //       但是我看对t.autoAborted赋值的代码，是先执行这个方法再对t.autoAborted赋值true，为什么不先赋值true再执行这个方法呢？
        // A：1、先执行这个方法再对t.autoAborted赋值true是因为保证internAbort方法执行完成后，
        //      才对t.autoAborted做标记，后续再执行这个方法时，发现t.autoAborted是true就直接返回，不再调用释放资源的方法了。
        //    2、t.autoAborted是全局变量，表示真正的状态。如果它是true，那这个事务就是true（自动回滚了），
        //       而autoAborted是调用方法时代码逻辑认为这里应该是true，至于是不是已经赋值为true了，还是依然为false，需要通过t.autoAborted这个全局变量来判断。
        //       比如abort方法中的autoAborted值为false，就是因为在代码逻辑中认为不应该是自动回滚的，所以是false。
        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    // 本类的read方法调用super.get(uid)时，会调用AbstractCache的实现方法，也就是这个getForCache
    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

}
