package com.lxpeak.mydb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.lxpeak.mydb.backend.common.SubArray;
import com.lxpeak.mydb.backend.dm.dataItem.DataItem;
import com.lxpeak.mydb.backend.tm.TransactionManagerImpl;
import com.lxpeak.mydb.backend.utils.Parser;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 *
 * 其中 LeafFlag 标记了该节点是否是个叶子节点；(1个字节)
 *     KeyNumber 为该节点中 key 的个数；(2个字节)
 *     SiblingUid 是其兄弟节点存储在 DM 中的 UID；(8个字节)
 *     son和key分别是8个字节。
 * 后续是穿插的子节点（SonN）和 KeyN。最后的一个 KeyN 始终为 MAX_VALUE，以此方便查找。
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    // 持有B+树结构的引用，DataItem的引用和SubArray的引用，用于方便快速修改数据和释放数据
    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    // 首先通过Parser.short2Byte((short)noKeys)得到一个内容是noKeys的字节数组，而且是short类型的，所以返回的是大小为2的字节数组
    // 然后将这个数组赋值给raw，得到的数组是2个字节，复制的长度也就是2个字节了
    static void setRawNoKeys(SubArray raw, int noKeys) {
        // 用 System.arraycopy 将noKeys以字节数组的形式复制到raw中
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        // 取出两个字节然后通过parseShort转成对应的数字
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    // 设置第K个子节点
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        // 其实最终效果就是给raw.raw复制
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    // 得到第K个子节点
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    // 设置第K个子节点的Key
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    // 得到第K个子节点的key
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    // 假如kth是2的话，有3个节点时分裂，那么1,2分给左节点（老节点），3分给了右节点（新节点）
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    // 从第k个开始移动一个[SON|UID]的空间,空出来的地方就是以后的第K个位置
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    // 生成一个根节点
    static byte[] newRootRaw(long left, long right, long key)  {
        //开辟NODE_SIZE大小的字节数组
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        //下面几个方法都是来填充开辟的字节数组的
        //第一个字节保存是否为叶子节点
        setRawIsLeaf(raw, false);
        //这个方法最终会强制类型转换为short型的，所以这里占用两个字节
        setRawNoKeys(raw, 2);
        // 这个方法用的是long型的，所以这里占用8个字节
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    // 生成一个空的根节点数据
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    // 寻找对应key的UID，如果找不到，则返回兄弟节点的UID
    public SearchNextRes searchNext(long key) {
        // 读锁
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            // key 个数
            int noKeys = getRawNoKeys(raw);
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i);
                // 遍历键，找到第一个大于目标键的位置，返回对应子节点UID；若无，返回兄弟节点UID
                if(key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    // leafSearchRange 方法在当前节点进行范围查找，范围是 [leftKey, rightKey]，
    // 这里约定如果 rightKey 大于等于该节点的最大的 key, 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点。
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            // key个数
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                // 找到比leftKey大的key后就跳出循环，开始从leftKey作为起点进行查找
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            // 如果该节点找完了，则返回兄弟节点的UID，方便继续搜索下一个节点。
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    // 插入键值对，若超出容量则分裂。
    // 若需要分裂，将后半部分键转移到新节点，更新兄弟指针，返回新节点信息供父节点处理
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        // 写日志前的预处理，加锁
        dataItem.before();
        try {
            success = insert(uid, key);
            if(!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if(needSplit()) {
                try {
                    // 分裂并返回新节点
                    // 注意，这里没有为res赋值siblingUid，上层方法里的if(iasr.siblingUid != 0)以此为依据进行分裂节点的父节点处理
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                // 如果有问题则撤销之前的预处理
                dataItem.unBefore();
            }
        }
    }

    // 将节点的key和uid插入当前节点的合适位置，并调整节点结构
    private boolean insert(long uid, long key) {
        // 得到keyNumber
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            // 得到第K个节点的KEY
            long ik = getRawKthKey(raw, kth);
            // 一直找到比key大或正好为key，然后跳出循环
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        // 如果这个节点找到最后一个key了且存在兄弟节点，不允许插入到末尾
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        // 调整节点数据，腾出空间
        // 是否是叶子节点
        if(getRawIfLeaf(raw)) {
            // 叶子节点
            // 将第k个节点及其以后的数据后移
            shiftRawKth(raw, kth);
            // 在第K个节点处插入key和uid
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
        } else {
            // 非叶子节点
            // 解释一下，叶子结点的比较好理解，就是将字节移动之后再添加上新的key和uid即可，但是非叶子节点比较麻烦。
            //  简单的B+树如图所示：
            //         [6 | 15]
            //        /   \    \
            //   [1,3]   [6,7] [15,18]
            // 1、非叶子节点的son保存的是对应key的左子树的uid，也就是非叶子节点的key=6，对应的son保存的uid是[1,3]，而叶子节点[6，7]的uid由非叶子节点的key=15保存。
            // 2、假如允许的key数量是4个的话，当叶子节点[6,7]添加了3个元素后变为[6,7,8,9,10]，此时由于大于最大数量4，所以叶子节点分裂为[6,7,8]和[9,10]两个叶子节点，
            //    同时需要对非叶子节点进行修改，增加一个指向新叶子节点的索引，最后得到如下结构：
            //                       [ 6 |  9 |  15  ]
            //                      /   /     \       \
            //                 [1,3]  [6,7,8]  [9,10] [15,18]
            // 其中非叶子节点15代表的就是变量kk，叶子节点[8,9,10]对应变量uid，非叶节点的8对应变量key。
            // 3、所以很多问题迎刃而解：
            // 3.1 为什么没有setRawKthSon(raw, uid, kth)这行代码？
            // A： 因为第k个节点本身是存在的，不需要重新赋值。
            //
            // 3.2 既然节点分裂了，那么为什么不需要对第K个节点重新赋值呢？
            // A： 因为在split方法里已经对旧节点和新节点的raw重新赋值了，旧节点分裂点key右边的部分都赋值给了新节点，也就是[1,2,3]->[1,2]和[3]

            // 得到原本第k个节点的key
            long kk = getRawKthKey(raw, kth);
            // 在第K个节点处插入key
            setRawKthKey(raw, key, kth);
            // 将第k+1个节点及其以后的数据后移
            shiftRawKth(raw, kth+1);
            // 在第K+1个节点处插入key和uid
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
        }
        // 设置keyNumber
        setRawNoKeys(raw, noKeys+1);
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    private SplitRes split() throws Exception {
        // 开辟新空间给分裂出的新节点
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        // 给新节点赋值
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        // 给新节点划分一半的key，分裂点的key属于旧节点
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        // 得到新节点UID
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);

        // 给老节点赋值
        setRawNoKeys(raw, BALANCE_NUMBER);
        // 老节点的兄弟节点是新节点
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
