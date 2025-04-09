package com.lxpeak.mydb.backend.parser;

import com.lxpeak.mydb.common.Error;

public class Tokenizer {
    private byte[] stat; //原始输入字节流
    private int pos; //当前解析位置
    private String currentToken;//当前缓存的词法单元
    private boolean flushToken;//标记是否需要重新解析下一个词法单元
    private Exception err;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    // 如果flushToken为true，调用next()解析新词法单元
    // 如果flushToken为false，返回当前缓存的词法单元currentToken
    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    // 标记当前词法单元已处理，下次 peek() 将解析新词法单元
    public void pop() {
        flushToken = true;
    }

    // 生成错误信息，标记解析失败的位置（例如 SELECT << FROM table）
    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }

    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    // 解析下一个词法单元，处理空白字符、符号、标识符、引号字符串等
    private String nextMetaState() throws Exception {
        // 排除掉所有空格、换行
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                return "";
            }
            if(!isBlank(b)) {
                break;
            }
            popByte();
        }
        // 拿到第一个有实际意义的字符
        byte b = peekByte();
        // 如果是符号则返回符号
        if(isSymbol(b)) {
            popByte();
            return new String(new byte[]{b});
        }
        // 如果是引号则解析引号字符串
        else if(b == '"' || b == '\'') {
            return nextQuoteState();
        }
        // 如果是标识符或数字则解析标识符或数字
        else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            // 这个两层if有点难看，修改了一下，不知道会不会出bug
            // if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
            //     if(b != null && isBlank(b)) {
            //         popByte();
            //     }
            //     return sb.toString();
            // }
            if(b == null){
                return sb.toString();
            }
            if(isBlank(b)){
                popByte();
                return sb.toString();
            }
            if(!(isAlphaBeta(b) || isDigit(b) || b == '_')){
                return sb.toString();
            }

            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
		b == ',' || b == '(' || b == ')');
    }

    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
