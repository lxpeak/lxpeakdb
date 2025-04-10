package com.lxpeak.mydb.backend.tbm;

import java.util.Arrays;
import java.util.List;

import com.google.common.primitives.Bytes;

import com.lxpeak.mydb.backend.im.BPlusTree;
import com.lxpeak.mydb.backend.parser.statement.SingleExpression;
import com.lxpeak.mydb.backend.tm.TransactionManagerImpl;
import com.lxpeak.mydb.backend.utils.Panic;
import com.lxpeak.mydb.backend.utils.ParseStringRes;
import com.lxpeak.mydb.backend.utils.Parser;
import com.lxpeak.mydb.common.Error;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 */
public class Field {
    long uid;
    private Table tb;
    String fieldName;
    String fieldType;
    private long index; //字段的索引标识符（如果字段有索引）
    private BPlusTree bt; //字段的 B+ 树索引对象（如果字段有索引）

    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            // 根据事务id(xid)和索引节点id(uid)得到真正数据的字节数组
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    private Field parseSelf(byte[] raw) {
        int position = 0;
        // 截取raw中的第一个字符串数据作为字段名
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        // 移动偏移量
        position += res.next;
        // (1) Arrays.copyOfRange(raw, position, raw.length)得到字段名后面的字节数组
        // (2) parseString会截取第一个字符串数据,现在得到的是"字段类型"字符串
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        // 移动偏移量
        position += res.next;
        // 如果这个字段没有索引则为 0。
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        if(index != 0) {
            try {
                // 加载B+树，其实就是create和open中的open
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /*
    * indexed：是否需要索引
    * */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        // 检查字段类型合法性
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        // 若需要索引则生成新的B+树
        if(indexed) {
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);
        return f;
    }

    // 将字段数据持久化到数据库
    private void persistSelf(long xid) throws Exception {
        // 由于需要使用对象变量，所以需要先保证当前的Field对象是赋值后的
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        // 将字段名、类型和索引UID拼接为字节数组，得到uid
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    public boolean isIndexed() {
        return index != 0;
    }

    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);
                break;
            case "string":
                raw = Parser.string2Byte((String)v);
                break;
        }
        return raw;
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
            .append(fieldName)
            .append(", ")
            .append(fieldType)
            .append(index!=0?", Index":", NoIndex")
            .append(")")
            .toString();
    }

    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch(exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                if(res.right > 0) {
                    res.right --;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }
}
