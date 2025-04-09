package com.lxpeak.mydb.backend.parser;

import java.util.ArrayList;
import java.util.List;

import com.lxpeak.mydb.backend.parser.statement.Create;
import com.lxpeak.mydb.backend.parser.statement.Drop;
import com.lxpeak.mydb.backend.parser.statement.Select;
import com.lxpeak.mydb.backend.parser.statement.Where;
import com.lxpeak.mydb.common.Error;
import com.lxpeak.mydb.backend.parser.statement.Abort;
import com.lxpeak.mydb.backend.parser.statement.Begin;
import com.lxpeak.mydb.backend.parser.statement.Commit;
import com.lxpeak.mydb.backend.parser.statement.Delete;
import com.lxpeak.mydb.backend.parser.statement.Insert;
import com.lxpeak.mydb.backend.parser.statement.Show;
import com.lxpeak.mydb.backend.parser.statement.SingleExpression;
import com.lxpeak.mydb.backend.parser.statement.Update;

/*
* SQL 语句语法如下：
* <begin statement>
*     begin [isolation level (read committed|repeatable read)]
*         begin isolation level read committed
*
* <commit statement>
*     commit
*
* <abort statement>
*     abort
*
* <create statement>
*     create table <table name>
*     <field name> <field type>
*     <field name> <field type>
*     ...
*     <field name> <field type>
*     [(index <field name list>)]
*         create table students
*         id int32,
*         name string,
*         age int32,
*         (index id name)
*
* <drop statement>
*     drop table <table name>
*         drop table students
*
* <select statement>
*     select (*|<field name list>) from <table name> [<where statement>]
*         select * from student where id = 1
*         select name from student where id > 1 and id < 4
*         select name, age, id from student where id = 12
*
* <insert statement>
*     insert into <table name> values <value list>
*         insert into student values 5 "Zhang SAN" 22
*
* <delete statement>
*     delete from <table name> <where statement>
*         delete from student where name = "Zhang San"
*
* <update statement>
*     update <table name> set <field name>=<value> [<where statement>]
*         update student set name = "ZYJ" where id = 5
*
* <where statement>
*     where <field name> (>|<|=) <value> [(and|or) <field name> (>|<|=) <value>]
*         where age > 10 or age < 3
*
* <field name> <table name>
*     [a-zA-Z][a-zA-Z0-9_]*
*
* <field type>
*     int32 int64 string
*
* <value>
*
*
* */
// 第九章
public class Parser {
    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            switch(token) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                default:
                    throw Error.InvalidCommandException;
            }
        } catch(Exception e) {
            statErr = e;
        }
        try {
            // 检查是否解析完全
            String next = tokenizer.peek();
            if(!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch(Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        if(statErr != null) {
            throw statErr;
        }
        return stat;
    }

    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            return new Show();
        }
        throw Error.InvalidCommandException;
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();
        tokenizer.pop();

        if(!"set".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        update.fieldName = tokenizer.peek();
        tokenizer.pop();

        if(!"=".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        update.value = tokenizer.peek();
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);
        return update;
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if(!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if(!"into".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        insert.tableName = tableName;
        tokenizer.pop();

        if(!"values".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> values = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }
        insert.values = values.toArray(new String[values.size()]);

        return insert;
    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        // 如果是*，继续都下一个token
        if("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            // 如果是字段(如id,name等)一直读到没有逗号然后跳出循环
            while(true) {
                String field = tokenizer.peek();
                if(!isName(field)) {
                    throw Error.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if(",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        // 1、List类有无参toArray方法，返回Object，需显式类型转换，所以可能出现ClassCastException
        // 2、new String[fields.size()]和new String[0]类似。
        //    Java会优化new String[0]，自动创建大小匹配的新数组，性能与指定大小写法相近，
        //    而new String[fields.size()]是显式指定数组大小，代码意图更明确。
        read.fields = fields.toArray(new String[fields.size()]);

        // 解析from语句
        if(!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        // 很明显，这是from后的表名
        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        // 解析where，可能为空
        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    // 解析条件表达式（如 WHERE id = 1 AND name = 'Alice'）
    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if(!"where".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        // 解析第一个条件（如 "id = 1"）
        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        // 解析逻辑运算符（如 "AND"）
        String logicOp = tokenizer.peek();
        if("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if(!isLogicOp(logicOp)) {
            throw Error.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        // 解析第二个条件
        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return where;
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();

        String field = tokenizer.peek();
        if(!isName(field)) {
            throw Error.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if(!isCmpOp(op)) {
            throw Error.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    /*
    * <create statement>
    * create table <table name>
    * <field name> <field type>
    * <field name> <field type>
    * ...
    * <field name> <field type>
    * [(index <field name list>)]
    *     create table students
    *     id int32,
    *     name string,
    *     age int32,
    *     (index id name)
    *
    * */
    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        Create create = new Create();
        // 解析表名
        String name = tokenizer.peek();
        if(!isName(name)) {
            throw Error.InvalidCommandException;
        }
        create.tableName = name;

        // 解析字段定义（如 "id int32, name string"）
        List<String> fNames = new ArrayList<>(); // 字段名(fieldName)
        List<String> fTypes = new ArrayList<>(); // 字段类型(fieldType)
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            // 根据create格式，有括号的是创建索引的语句，所以直接跳出，进行后续的索引处理
            if("(".equals(field)) {
                break;
            }

            if(!isName(field)) {
                throw Error.InvalidCommandException;
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if(!isType(fieldType)) {
                throw Error.InvalidCommandException;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();

            String next = tokenizer.peek();
            if(",".equals(next)) {
                continue;
            } else if("".equals(next)) {
                throw Error.TableNoIndexException;
            } else if("(".equals(next)) {
                // 根据create格式，有括号的是创建索引的语句，所以直接跳出，进行后续的索引处理
                break;
            } else {
                throw Error.InvalidCommandException;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        // 解析索引（如 "(id, name)"）
        // 虽然格式里写的索引是可选的，但是代码里不是
        tokenizer.pop();
        if(!"index".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> indexes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if(")".equals(field)) {
                break;
            }
            if(!isName(field)) {
                throw Error.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return create;
    }

    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
        "string".equals(tp));
    }

    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Abort();
    }

    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Commit();
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        if("".equals(isolation)) {
            return begin;
        }
        if(!"isolation".equals(isolation)) {
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();
        String level = tokenizer.peek();
        if(!"level".equals(level)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("committed".equals(tmp2)) {
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else if("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else {
            throw Error.InvalidCommandException;
        }
    }

    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }
}
