package com.xy.lr.tics.spark.sql;

import com.xy.lr.tics.properties.TICSInfoJava;
import org.zeromq.ZMQ;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by xylr on 15-4-12.
 */
public class SparkSQLClientJava {
//    private TICSInfoJava ticsInfo = null;
    private ZMQ.Context sparkSQLClientContext = ZMQ.context(1);
    private ZMQ.Socket sparkSQLClientSocket = sparkSQLClientContext.socket(ZMQ.REQ);

    public SparkSQLClientJava(String path){
        //配置文件
        TICSInfoJava ticsInfo = new TICSInfoJava(path);
        //连接SparkSQLServer
        sparkSQLClientSocket.connect("tcp://" + ticsInfo.getSparkSQLServerUrl() +
                ":" + ticsInfo.getSparkSQLServerPort());
    }
    public String getCurrentTime(){
        Date date = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(date);
    }
    public byte[] makeRequest(String req){
        String requestString = req + " ";
        byte[] request = requestString.getBytes();
        request[request.length - 1] = 0;
        return request;
    }
    public String getReply(byte[] rep){
        return new String(rep, 0, rep.length - 1);
    }
    public String test(String req){
        System.out.println("Connecting to [ SparkSQLServer ] at " + getCurrentTime());

        byte[] request = makeRequest(req);

        System.out.println("Sending request : " + req);
        //发送查询请求
        sparkSQLClientSocket.send(request, 0);

        //接受查询结果
        byte[] reply = sparkSQLClientSocket.recv(0);
        System.out.println("Received reply " + ": [" + getReply(reply) + "]");

        //返回结果
        return getReply(reply);
    }
    public String sql(String query){
        return test("sql" + query);
    }
    public String getCarGraph(String carNumber){
        return test("CG" + carNumber);
    }
}
