package com.lxpeak.lxpeakdb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import com.lxpeak.lxpeakdb.backend.utils.Panic;
import com.lxpeak.lxpeakdb.common.Error;
import com.lxpeak.lxpeakdb.backend.utils.Parser;

/**
 * 日志文件读写
 * 
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * 
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;
    
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        // 截断文件到正常日志的末尾
        try {
            // 这里的position是由while循环中的internNext()计算得出的
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            // 设置文件指针的位置，使得接下来的读写操作从该位置开始
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    // 计算出一个校验码
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    @Override
    public void log(byte[] data) {
        // 得到一个包装好的日志二进制数据
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            // 写入日志文件
            fc.position(fc.size());
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        // 更新总的校验码
        updateXChecksum(log);
    }

    // 更新总的校验码
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    // 将日志大小、校验码、数据包装成一个字节数组
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    private byte[] internNext() {
        // 当前位置+数据 >= 文件大小时
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            // 前四个字节是某条日志的大小Size
            fc.position(position);
            // 通过fc将文件信息读取到tmp中。（虽然我一直觉得应该叫fc把日志信息写入到tmp这个ByteBuffer中，不过在逻辑上确实是对文件进行读操作）
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        // 检查是否越界
        if(position + size + OF_DATA > fileSize) {
            return null;
        }

        // 读取某条日志的校验码
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        // 计算出的校验码和日志中的校验码进行核对
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if(checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    // 得到日志文件中的一条日志记录
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    
}
