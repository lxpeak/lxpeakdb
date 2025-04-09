package com.lxpeak.mydb.backend.server;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import com.lxpeak.mydb.backend.dm.DataManager;
import com.lxpeak.mydb.backend.tm.TransactionManager;
import com.lxpeak.mydb.backend.vm.VersionManager;
import org.junit.Test;

import com.lxpeak.mydb.backend.tbm.TableManager;

public class ExecutorTest {
    String path1 = "D://mydb/dbTest/ExecutorTestDB";
    String path2 = "D://mydb/dbTest/ExecutorTestDB2";
    long mem = (1 << 20) * 64;

    byte[] CREATE_TABLE = "create table test_table id int32 (index id)".getBytes();
    byte[] INSERT = "insert into test_table values 2333".getBytes();

    private Executor testCreate(String path) throws Exception {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, mem, tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        TableManager tbm = TableManager.create(path, vm, dm);
        Executor exe = new Executor(tbm);
        exe.execute(CREATE_TABLE);
        return exe;
    }

    private void testInsert(Executor exe, int times, int no) throws Exception {
        for (int i = 0; i < times; i++) {
            System.out.print(no+":"+i + ":");
            exe.execute(INSERT);
        }
    }

    @Test
    public void testInsert10000() throws Exception {
        Executor exe = testCreate(path1);
        testInsert(exe, 10000, 1);
        new File(path1 + ".db").delete();
        new File(path1 + ".bt").delete();
        new File(path1 + ".log").delete();
        new File(path1 + ".xid").delete();
    }

    private void testMultiInsert(int total, int noWorkers) throws Exception {
        Executor exe = testCreate(path2);
        // 这里必须用不同的executor，否则会出现并发问题
        TableManager tbm = exe.tbm;
        int w = total/noWorkers;
        CountDownLatch cdl = new CountDownLatch(noWorkers);
        for(int i = 0; i < noWorkers; i ++) {
            final int no = i;
            new Thread(new Runnable(){
                @Override
                public void run() {
                    try {
                        testInsert(new Executor(tbm), w, no);
                        cdl.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        cdl.await();
    }

    @Test
    public void test100000With4() throws Exception {
        testMultiInsert(10000, 4);
        new File(path2 + ".db").delete();
        new File(path2 + ".bt").delete();
        new File(path2 + ".log").delete();
        new File(path2 + ".xid").delete();
    }
}
