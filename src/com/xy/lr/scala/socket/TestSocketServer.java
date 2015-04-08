package com.xy.lr.scala.socket;

/**
 * Created by xylr on 15-3-5.
 */
public class TestSocketServer {
    public static void main(String[] argvs){
        SocketServers server = new SocketServers(12345);
        server.beginListen("100");
    }
}
