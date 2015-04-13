package com.xy.lr.tics.test;

import com.xy.lr.tics.dezhouserver.DeZhouSocketServerJava;

/**
 * Created by xylr on 15-3-5.
 */
public class TestSocketServer {
    public static void main(String[] argvs){
        DeZhouSocketServerJava server = new DeZhouSocketServerJava(12345);
        server.beginListen("100");
    }
}
