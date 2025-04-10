package com.lxpeak.mydb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.lxpeak.mydb.backend.common.SubArray;
import com.lxpeak.mydb.backend.dm.DataManager;
import com.lxpeak.mydb.backend.tm.TransactionManagerImpl;
import com.lxpeak.mydb.backend.utils.Parser;
import com.lxpeak.mydb.backend.dm.dataItem.DataItem;

/*
* 由于 B+ 树在插入删除时，会动态调整，根节点不是固定节点，于是设置一个 bootDataItem，该 DataItem 中存储了根节点的 UID。
* 可以注意到，IM 在操作 DM 时，使用的事务都是 SUPER_XID。
* ------------------------------------------------------------------------------------------------------
*
* 注意：IM没有提供删除索引的能力。当上层模块通过VM删除某个Entry时，实际的操作是设置其XMAX。
* 如果不去删除对应索引的话，当后续再次尝试读取该Entry时，是可以通过索引寻找到的，但是由于设置了XMAX，所以会在寻找不到合适的版本时返回一个找不到对应内容的错误。
* */
public class BPlusTree {
    DataManager dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;

    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    // 得到root节点的uid
    private long rootUid() {
        bootLock.lock();
        try {
            // 从bootDataItem中得到根节点数据
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            // before进行加锁和保存数据前的预处理，after进行日志操作并解锁
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    // 递归找叶子节点，先横着找每个兄弟节点，找到比目标key大的值后，进入下一层继续横着找
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    private long searchNext(long nodeUid, long key) throws Exception {
        // 最后一个值时MAX_VALUE所以一定会跳出循环
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        // 找到范围查找里的左侧边界leftKey
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            // 如果返回的结果里有兄弟节点，则继续查找兄弟节点是否有属于[leftKey, rightKey]的值
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        // 根据uid读取节点数据
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {
            // 插入键值对，处理节点分裂
            // 该方法会处理完所有的兄弟节点后返回
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key);
            // 递归的插入,直到节点为叶子节点后,开始一层层退出递归
            InsertRes ir = insert(next, uid, key);
            // 如果有分裂操作，则ir.newNode != 0，返回的newNode是分裂后的新兄弟节点，需要修改他的父节点
            if(ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            // while循环会一直处理添加操作到最后一个兄弟节点,如果没有兄弟节点了,则退出
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
