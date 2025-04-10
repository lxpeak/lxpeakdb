package com.lxpeak.lxpeakdb.client;

import com.lxpeak.lxpeakdb.transport.Packager;
import com.lxpeak.lxpeakdb.transport.Package;

public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    // stat: 输入的sql命令
    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        // 发送sql语句，然后接收处理结果
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
