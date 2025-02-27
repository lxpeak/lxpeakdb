package com.lxpeak.mydb.backend.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.primitives.Bytes;

// 返回相应的字节数组 或 解析字节数组返回相应结果
public class Parser {

    /*
    * 这里的short2byte等方法在开辟空间时都是用的xxx.SIZE/Byte.SIZE，意思是要开辟符合它大小的字节空间，
    * 比如我想把int a = 1保存起来，首先要变成byte数组的形式，那么需要多大空间呢？因为a是int型的，所以需要4个字节，
    * 所以在int2Byte方法里就是Integer.SIZE / Byte.SIZE。（这里的Integer.SIZE保存的是bit的大小，也就是4*8=32位）
    * 所以int2Byte得到的数组就是大小为4的字节数组，因为int就是四个字节
    * */
    public static byte[] short2Byte(short value) {
        // 开辟大小为16/8=2的字节数组，并将value以2个byte的长度写入ByteBuffer中，最后以byte[]形式返回
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    public static byte[] int2Byte(int value) {
        // 开辟大小为32/8=4的字节数组，并将value以4个byte的长度写入ByteBuffer中，最后以byte[]形式返回
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    public static byte[] long2Byte(long value) {
        // 开辟大小为64/8=8的字节数组，并将value以8个byte的长度写入ByteBuffer中，最后以byte[]形式返回
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        // todo 为什么这里也要length+4
        return new ParseStringRes(str, length+4);
    }

    // 字符串转字节需要这种形式：[字符串长度][字符串内容]
    public static byte[] string2Byte(String str) {
        // 得到一个大小为4的字节数组，保存字符串长度
        byte[] l = int2Byte(str.length());
        // 将得到的数组与str转换成的字节数组连接起来
        return Bytes.concat(l, str.getBytes());
    }

    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }

}
