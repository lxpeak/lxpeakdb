package com.lxpeak.mydb.backend.tm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.lxpeak.mydb.backend.utils.Panic;
import com.lxpeak.mydb.common.Error;

public interface TransactionManager {
    long begin();
    void commit(long xid);
    void abort(long xid);
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close();

    public static TransactionManagerImpl create(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        // 参考：高速读写是因为0拷贝：https://blog.csdn.net/jiaoshi5167/article/details/130213512
        //      FileChannel结合MappedByteBuffer实现高效读写: https://blog.csdn.net/weixin_53589418/article/details/112121624
        FileChannel fc = null;
        // RandomAccessFile 可以说是 Java 体系中功能最为丰富的文件操作类，相比之前介绍的通过字节流或者字符流接口方式读写文件，
        // RandomAccessFile 类可以跳转到文件的任意位置处进行读写数据，而无需把文件从头读到尾，但是该类仅限于操作文件，不能访问其他的 IO 设备，如网络、内存映像等。
        // 参考：https://zhuanlan.zhihu.com/p/641985183
        //      https://blog.csdn.net/akon_vm/article/details/7429245
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    public static TransactionManagerImpl open(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
