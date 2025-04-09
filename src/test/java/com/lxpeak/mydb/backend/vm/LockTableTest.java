package com.lxpeak.mydb.backend.vm;

import static org.junit.Assert.assertThrows;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

import com.lxpeak.mydb.backend.utils.Panic;
import org.junit.Test;

public class LockTableTest {

    @Test
    public void testLockTable() {
        LockTable lt = new LockTable();
        try {
            lt.add(1, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            lt.add(2, 2);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            lt.add(2, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }

        assertThrows(RuntimeException.class, ()->lt.add(1, 2));
    }

    @Test
    public void testLockTable2() {
        LockTable lt = new LockTable();
        for(long i = 1; i <= 100; i ++) {
            try {
                Semaphore o = lt.add(i, i);
                if(o != null) {
                    Runnable r = () -> {
                        try {
                            o.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        o.release();
                    };
                    new Thread(r).start();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        for(long i = 1; i <= 99; i ++) {
            try {
                Semaphore o = lt.add(i, i+1);
                if(o != null) {
                    Runnable r = () -> {
                        try {
                            o.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        o.release();
                    };
                    new Thread(r).start();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        assertThrows(RuntimeException.class, ()->lt.add(100, 1));
        lt.remove(23);

        try {
            lt.add(100, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }
}
