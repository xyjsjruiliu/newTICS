package com.xy.lr.tics.properties;

import java.io.*;
import java.util.Properties;

/**
 * Created by xylr on 15-4-12.
 */
public class TICSInfoJava {
    private Properties p;
    //SparkSQLServer 的端口号
    private String SparkSQLServerPort;
    //卡口服务器的端口号
    private String DeZhouServerPort;
    //SparkSQLServer 的url地址
    private String SparkSQLServerUrl;
    //卡口服务器地址
    private String DeZhouServerUrl;
    public TICSInfoJava(String path){
        File f = new File(path);
        p = new Properties();
        BufferedInputStream in = null;
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(path);
            in = new BufferedInputStream(fis);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            p.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //导入配置文件数据
        this.SparkSQLServerPort = getProperties(p, "SparkSQLServerPort");
        this.DeZhouServerPort = getProperties(p, "DeZhouServerPort");
        this.SparkSQLServerUrl = getProperties(p, "SparkSQLServerUrl");
        this.DeZhouServerUrl = getProperties(p, "DeZhouServerUrl");

        /*
        String propertiesFileString = "Load TICSSparkSQLInfo.properties from " + f.getAbsoluteFile();
        String ss = (propertiesFileString.length() + 2) ** "-";

        String sql = "SparkSQLServerPort: " + SparkSQLServerPort;
        String dzs = "DeZhouServerPort: " + DeZhouServerPort;
        String sssu = "SparkSQLServerUrl: " + SparkSQLServerUrl;
        String dzsu = "DeZhouServerUrl: " + DeZhouServerUrl;

        String bl = " " * (ss.length() - sql.length() - 2);
        String dzsbl = " " * (ss.length() - dzs.length() - 2);
        String sssul = " " * (ss.length() - sssu.length() - 2);
        String dzsul = " " * (ss.length() - dzsu.length() - 2);

        //输出导入的配置文件信息
        System.out.println(
                ss + "\n" +
                        "|" + propertiesFileString + "|" + "\n" + ss + "\n" +
                        "|" + sql + bl + "|" + "\n" +
                        "|" + dzs + dzsbl + "|" + "\n" +
                        "|" + sssu + sssul + "|" + "\n" +
                        "|" + dzsu + dzsul + "|" + "\n" +
                        ss);
                        */
    }
    public String getProperties(Properties p, String pro){
        return p.getProperty(pro);
    }
    public String getDeZhouServerUrl(){
        return DeZhouServerUrl;
    }
    public String getSparkSQLServerUrl(){
        return SparkSQLServerUrl;
    }
    public String getSparkSQLServerPort(){
        return SparkSQLServerPort;
    }
    public String getDeZhouServerPort(){
        return DeZhouServerPort;
    }
}
