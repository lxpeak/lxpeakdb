package com.lxpeak.lxpeakdb.backend.tbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Bytes;

import com.lxpeak.lxpeakdb.backend.parser.statement.Create;
import com.lxpeak.lxpeakdb.backend.parser.statement.Delete;
import com.lxpeak.lxpeakdb.backend.parser.statement.Select;
import com.lxpeak.lxpeakdb.backend.parser.statement.Where;
import com.lxpeak.lxpeakdb.backend.utils.Panic;
import com.lxpeak.lxpeakdb.backend.utils.ParseStringRes;
import com.lxpeak.lxpeakdb.common.Error;
import com.lxpeak.lxpeakdb.backend.parser.statement.Insert;
import com.lxpeak.lxpeakdb.backend.parser.statement.Update;
import com.lxpeak.lxpeakdb.backend.tbm.Field.ParseValueRes;
import com.lxpeak.lxpeakdb.backend.tm.TransactionManagerImpl;
import com.lxpeak.lxpeakdb.backend.utils.Parser;

/**
 * Table维护了表结构
 * 一个数据库中存在多张表，TBM使用链表的形式将其组织起来，每一张表都保存一个指向下一张表的UID。表的二进制结构如下：
 * [TableName][NextTable（8字节）]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 *
 * ----------------------------------------------------
 * 对表和字段的操作，有一个很重要的步骤，就是计算Where条件的范围，目前LxPeakDB的Where只支持两个条件的与和或。
 * 例如有条件的Delete，计算Where，最终就需要获取到条件范围内所有的UID。LxPeakDB只支持已索引字段作为Where的条件。
 * 计算Where的范围，具体可以查看Table的parseWhere()和calWhere()方法，以及Field类的calExp()方法。
 *
 */
public class Table {
    TableManager tbm;
    long uid;
    String name;
    byte status;
    // 下一个table的Uid
    long nextUid;
    List<Field> fields = new ArrayList<>();

    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for(int i = 0; i < create.fieldName.length; i ++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            // 是否有需要建立索引的字段
            for(int j = 0; j < create.index.length; j ++) {
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        return tb.persistSelf(xid);
    }

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /*
    * 表的二进制结构如下：
    * [TableName][NextTable]
    * [Field1Uid][Field2Uid]...[FieldNUid]
    * */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        // 字符串和下一个要读取的位置
        name = res.str;
        position += res.next;
        // 根据表的二进制结构得到NextTable的Uid
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;

        // 遍历得到全部field
        while(position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
            position += 8;
            fields.add(Field.loadField(this, uid));
            // System.out.println("Table对象是："+this);
        }
        return this;
    }

    // 把表名、下一个表的Uid、字段的字节数组（由Field类提供）拼接成新的字节数组然后交给VM进行处理
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }

    public int update(long xid, Update update) throws Exception {
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        for (Field f : fields) {
            if(f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if(fd == null) {
            throw Error.FieldNotFoundException;
        }
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            // 1.根据uid找到对应的字段
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;

            // 2.删除这个字段的内容
            ((TableManagerImpl)tbm).vm.delete(xid, uid);

            // 解析读取的字节数组
            Map<String, Object> entry = parseEntry(raw);
            // 把新值放进去
            entry.put(fd.fieldName, value);
            // 将新的内容变成字节数组
            raw = entry2Raw(entry);
            // 3.添加这个字段的内容
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);

            count ++;

            // 重新添加索引字段到B+树
            for (Field field : fields) {
                if(field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    public String read(long xid, Select read) throws Exception {
        // 处理where条件的查询条件,得到对某字段的查询范围,通过B+树范围查询后得到UID集合
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        // 写日志以及执行插入操作
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        for (Field field : fields) {
            if(field.isIndexed()) {
                // 将有索引的字段放进B+树中
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    // 如果该表有5个字段,则values数组会传5个值,将字段名和字段值依次放进entry中
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if(values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    private List<Long> parseWhere(Where where) throws Exception {
        // 最多两个判断条件，第一个判断条件的搜索范围[l0,r0], 第二个判断条件的搜索范围[l1,r1]
        long l0=0, r0=0, l1=0, r1=0;
        boolean single = false;
        Field fd = null;
        // 如果没有WHERE条件，默认选择第一个有索引的字段的全部范围
        if(where == null) {
            for (Field field : fields) {
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        }

        // 找到 WHERE 条件中涉及的字段（必须是已索引的字段）
        else {
            for (Field field : fields) {
                if(field.fieldName.equals(where.singleExp1.field)) {
                    if(!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if(fd == null) {
                throw Error.FieldNotFoundException;
            }
            // 计算条件的搜索范围
            CalWhereRes res = calWhere(fd, where);
            // 第一个判断条件的搜索范围[l0,r0], 第二个判断条件的搜索范围[l1,r1]
            l0 = res.l0; r0 = res.r0;
            l1 = res.l1; r1 = res.r1;
            single = res.single;
        }
        // 调用 B+ 树查询满足范围的 UID 列表
        List<Long> uids = fd.search(l0, r0);
        if(!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch(where.logicOp) {
            // 单条件（如 age > 25）
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                // 第一个条件的搜索范围[l0,r0]
                res.l0 = r.left; res.r0 = r.right;
                break;

            // OR 条件（如 age < 20 OR age > 30）
            case "or":
                // 第一个条件的搜索范围[l0,r0], 第二个条件的搜索范围[l1,r1]
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                break;

            // AND 条件（如 age > 20 AND age < 30）
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                // 取交集
                if(res.l1 > res.l0) res.l0 = res.l1;
                if(res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == fields.size()-1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
