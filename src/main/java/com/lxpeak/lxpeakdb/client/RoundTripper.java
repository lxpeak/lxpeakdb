package com.lxpeak.lxpeakdb.client;

import com.lxpeak.lxpeakdb.transport.Packager;
import com.lxpeak.lxpeakdb.transport.Package;

public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    // RoundTripper 类实际上实现了单次收发动作
    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
