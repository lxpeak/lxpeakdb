package com.lxpeak.mydb.backend.vm;

import com.lxpeak.mydb.backend.tm.TransactionManager;

public class Visibility {

    //第七章
    //1、由于读已提交（RC）总是基于最新数据更新，跳跃在逻辑上不会发生，因为事务总能感知到中间版本。
    //2、版本跳跃即另一个事务已经修改了数据并且当前事务不可见该修改
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /*
    * 读已提交
    * TransactionManager tm管理事务；
    * Transaction t代表当前事务；
    * Entry e代表数据库中的某一行或某个数据条目
    * -----------------------------------------------------
    * xmin通常是创建该行的事务ID，xmax是删除或锁定该行的事务ID，如果为0可能表示未被删除或锁定。
    *
    * 该方法的逻辑是：
    * 1、当前事务t来查找数据Entry e，如果xmin是已提交的，说明创建该数据e的事务已提交，意味着该数据e在逻辑（读已提交）上是存在的，所以应该被当前事务t读到；
    * 2、如果xmax没有则表示没有事务想要删除该数据e；
    * 3、如果xmax存在，则要判断一下这个xmax是否已经被提交，也就是删除该数据e的事务是否已经被提交，
    *    如果没有提交，表示该数据在目前是应该被看到的（读已提交），如果事务xmax是已提交的，说明当前事务t不应该能看到数据e了。
    *
    * */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //当前事务创建了这个条目，并且还没有被删除，所以可见
        if(xmin == xid && xmax == 0) return true;

        /*
        * 如果xmax是0，意味着该条目由已提交的事务创建，且未被删除，所以可见。
        * 如果xmax不等于当前事务的xid，那么进一步检查xmax是否未提交，如果是未提交的话返回true。
        * 因为如果该条目被另一个未提交的事务删除或锁定，那么当前事务仍然可以看到它。反之，如果xmax已提交，则可能不可见。
        * */
        if(tm.isCommitted(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }


    /*
    * 可重复读
    * TransactionManager tm管理事务；
    * Transaction t代表当前事务；
    * Entry e代表数据库中的某一行或某个数据条目
    * -----------------------------------------------------
    * 判断条件比读已提交多了两个条件
    * 1、忽略在本事务之后开始的事务的数据;
    * 2、忽略本事务开始时还是 active 状态的事务的数据，也就是忽略此时已经开始事务但还没有提交的事务。
    *    因为本事务开始的时候可能有其他事务正在进行中，但还没有提交，期间修改的数据不应该被当前事务读到。
    * */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
