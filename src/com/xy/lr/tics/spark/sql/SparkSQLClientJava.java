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
    //zeroMQ
    private ZMQ.Context sparkSQLClientContext = ZMQ.context(1);
    //设置模式
    private ZMQ.Socket sparkSQLClientSocket = sparkSQLClientContext.socket(ZMQ.REQ);

    public SparkSQLClientJava(String path){
        //配置文件
        TICSInfoJava ticsInfo = new TICSInfoJava(path);
        //连接SparkSQLServer
        sparkSQLClientSocket.connect("tcp://" + ticsInfo.getSparkSQLServerUrl() +
                ":" + ticsInfo.getSparkSQLServerPort());
    }
    //获取当前时间
    private String getCurrentTime(){
        Date date = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(date);
    }
    //生成查询请求
    private byte[] makeRequest(String req){
        String requestString = req + " ";
        byte[] request = requestString.getBytes();
        request[request.length - 1] = 0;
        return request;
    }
    //解析查询结果
    private String getReply(byte[] rep){
        return new String(rep, 0, rep.length - 1);
    }
    private String test(String req){
        System.out.println("Connecting to [ SparkSQLServer ] at " + getCurrentTime());

        //生成查询
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
    //从黑名单中查询车辆轨迹
    public String sqlGrapgByBlackList(String carNumber){
        return test("GBL" + carNumber);
    }
    //获取黑名单列表
    public String sqlBlackList(){
        return test("BL");
    }
    //使用sql查询车辆信息表
    public String sql(String query){
        return test("sql" + query);
    }
    //通过车牌号查询车辆轨迹
    public String getCarGraph(String carNumber){
        return test("CG" + carNumber);
    }
}
