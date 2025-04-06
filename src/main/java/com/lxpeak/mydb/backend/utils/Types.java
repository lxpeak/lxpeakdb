package com.lxpeak.mydb.backend.utils;

public class Types {
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        // [u0 (32位) | u1 (32位)]。
        // u0<<32后得到的是[u0 (32位) | 0 (32位)]，将u1和低32位的0进行或运算，得到最终的uid
        return u0 << 32 | u1;
    }
}
