package com.xy.lr.tics.spark;

import java.io.*;
import java.net.*;

public class DeZhouClientJava {
    static Socket client;

    public DeZhouClientJava(String site, int port){
        try{
            client = new Socket(site,port);
//            System.out.println("Client is created! site:"+site+" port:"+port);
        }catch (UnknownHostException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public String getMsg(){
        try{
            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
//            PrintWriter out = new PrintWriter(client.getOutputStream());
//            out.println(msg);
//            out.flush();
            return in.readLine();
        }catch(IOException e){
            e.printStackTrace();
        }
        return "";
    }
    public void closeSocket(){
        try{
            client.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}