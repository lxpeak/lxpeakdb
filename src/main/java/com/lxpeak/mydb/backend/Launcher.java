package com.lxpeak.mydb.backend;

import com.lxpeak.mydb.backend.dm.DataManager;
import com.lxpeak.mydb.backend.server.Server;
import com.lxpeak.mydb.backend.utils.Panic;
import com.lxpeak.mydb.backend.vm.VersionManager;
import com.lxpeak.mydb.common.Error;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.lxpeak.mydb.backend.tbm.TableManager;
import com.lxpeak.mydb.backend.tm.TransactionManager;
import com.lxpeak.mydb.backend.vm.VersionManagerImpl;

public class Launcher {

    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1<<20)*64;
    public static final long KB = 1 << 10;
	public static final long MB = 1 << 20;
	public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        /*
        * 为了在命令行中执行命令，所以用apache.commons.cli提供的方法预设几个命令
        * */
        Options options = new Options();
        options.addOption("create", true, "-create DBPath");
        options.addOption("open", true, "-open DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);

        // 创建数据库
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        // 启动已有的数据库
        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    /*
    * 一开始先create再open
    * */
    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        // todo 感觉这里的close语句应该放进create里，或者封装到新的方法里，看起来不美观
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
