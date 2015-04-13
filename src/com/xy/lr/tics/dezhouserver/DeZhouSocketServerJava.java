package com.xy.lr.tics.dezhouserver;
import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DeZhouSocketServerJava {
    ServerSocket sever;
    public DeZhouSocketServerJava(int port){
        try{
            sever = new ServerSocket(port);
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    public String  getCurrentTime(){
        Date date = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        return df.format(date);
    }
    public void beginListen(String carNUmber){
        while(true){
            Thread t = null;
            try{
                final Socket socket = sever.accept();
                System.out.println("Got client connect from : " + socket.getInetAddress());
                System.out.println("Start Send Meg");
                final String CarNumber = carNUmber;
                t = new Thread(){
                    public void run(){
                        try{
                            int i = 0;
                            PrintWriter out = new PrintWriter(socket.getOutputStream());
                            while(true){
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                String time = getCurrentTime();
                                out.println("start" + "\t" + CarNumber + "\t" + time);
//                                System.out.println("Hello!World@" + "\t" + i);
                                i++;
                                out.flush();
                                if(socket.isClosed()){
                                    break;
                                }
                            }
                            socket.close();
                        }catch(IOException e) {
                            e.printStackTrace();
                        }
                    }
                };
                t.start();

            }catch(IOException e){
                e.printStackTrace();
                t.stop();
            }
        }
    }
}
