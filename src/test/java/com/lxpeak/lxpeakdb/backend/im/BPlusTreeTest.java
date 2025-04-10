package com.lxpeak.lxpeakdb.backend.im;

import java.io.File;
import java.util.List;

import com.lxpeak.lxpeakdb.backend.dm.DataManager;
import com.lxpeak.lxpeakdb.backend.dm.pageCache.PageCache;
import com.lxpeak.lxpeakdb.backend.tm.MockTransactionManager;
import org.junit.Test;

import com.lxpeak.lxpeakdb.backend.tm.TransactionManager;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("D://lxpeakdb/dbTest/TestTreeSingle", PageCache.PAGE_SIZE*10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 10000;
        for(int i = lim-1; i >= 0; i --) {
            tree.insert(i, i);
        }

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

        assert new File("D://lxpeakdb/dbTest/TestTreeSingle.db").delete();
        assert new File("D://lxpeakdb/dbTest/TestTreeSingle.log").delete();
    }
}
