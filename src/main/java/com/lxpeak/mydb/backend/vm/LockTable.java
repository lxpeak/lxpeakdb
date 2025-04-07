package com.lxpeak.mydb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.lxpeak.mydb.common.Error;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 * ---------------------------------------------
 *
 * 2PL 会阻塞事务，直至持有锁的线程释放锁。可以将这种等待关系抽象成有向边，
 * 例如 Tj 在等待 Ti，就可以表示为 Tj --> Ti。
 * 这样，无数有向边就可以形成一个图（不一定是连通图）。检测死锁也就简单了，只需要查看这个图中是否有环即可。
 * */
public class LockTable {
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    public Lock add(long xid, long uid) throws Exception {
        /*
        * 当尝试为一个事务添加一个资源的锁时，首先检查该事务是否已经持有该资源，
        * 1、如果是的话直接返回null，表示不需要等待。
        * 2、如果资源未被持有（u2x中不存在该UID），则将该资源分配给该事务，并更新x2u和u2x。
        * 3、否则，将该事务加入到等待该资源的队列中，并检测是否会导致死锁。如果有死锁，则回滚并抛出异常；否则为该事务创建一个锁并返回。
        * */
        lock.lock();
        try {
            // 事务xid是否持有uid这个资源
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            // 这个资源(uid)是否被某个事务(xid)持有
            if(!u2x.containsKey(uid)) {
                // 如果资源未被持有，则将该资源分配给该事务，并更新x2u和u2x
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            // 如果资源被其他事务持有，则将该事务加入到等待该资源的队列中
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);
            // 检测是否会导致死锁
            if(hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;

        } finally {
            lock.unlock();
        }
    }

    //在一个事务 commit 或者 abort 时，就可以释放所有它持有的锁，并将自身从等待图中删除。
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;

        while(l.size() > 0) {
            long xid = l.remove(0);
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(l.size() == 0) wait.remove(uid);
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    /*
    * 查找图中是否有环的算法非常简单，就是一个深搜，只是需要注意这个图不一定是连通图。
    * 思路就是为每个节点设置一个访问戳，一开始初始化为null，随后遍历所有节点，以每个非null且未被遍历的节点作为根进行深搜，
    * 并将深搜该连通图中遇到的所有节点都设置为同一个数字，不同的连通图数字不同。这样，如果在遍历某个图时，遇到了之前遍历过的节点，说明出现了环。
    * */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        // 染色用的标记
        stamp = 1;
        for(long xid : x2u.keySet()) {
            // 如果xid不存在，s=null，因为是包装类。
            Integer s = xidStamp.get(xid);
            //第一个条件判断是不是初始值（初始值是null），第二个条件判断是不是被访问过了
            if(s != null && s > 0) {
                continue;
            }
            stamp ++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    /*
    * 两个if判断的区别，例如，第一次dfs发现A依赖B，B依赖C，第二次dfs发现D依赖C，
    * 这种情况就是第二个if判断里的stp < stamp，并没有死锁，因为虽然B和D都依赖C，但是C是可以释放资源的。
    * A->B->C
    * D->C
    * ----------------
    * 但是如果是这样的话，A依赖B，B依赖C，C依赖A，三个都没法释放资源就死锁了
    * A->B->C->A
    * */
    // 死锁返回true
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        // 死锁了
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        // xid依赖这个uid，然后通过下面的u2x.get(uid)方法获取持有这个uid的xid，然后在进行一次dfs，这样就能把这条依赖链全部用stamp染色。
        // 在dfs方法外面，只要将所有的x2u中的x遍历完，如果没有读到已经染色的节点的话，就说明没有死锁。
        Long uid = waitU.get(xid);
        if(uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null; // Java中的`assert`关键字默认是不启用的，需要在运行时添加`-ea`参数来启用断言。因此，在生产环境中，这个断言可能不会生效，但它在开发和测试阶段有助于发现程序中的逻辑错误。
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

}
