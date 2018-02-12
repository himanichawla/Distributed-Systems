package com.himani.chawla.distributed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class Utils {

    public static byte[] convertMessageToBytes(Message msg) {
        byte[] buffer = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(msg);
            out.flush();
            buffer = bos.toByteArray();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return buffer;
    }

    public static Message convertBytesToMessage(byte[] buffer) {
        Message msg = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(buffer);
            ois = new ObjectInputStream(bis);
            msg = (Message) ois.readObject();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return msg;
    }

    public static Integer convertToTimeStamp(String time) {
        String[] minsSecs = time.split(":");
        int mins = Integer.parseInt(minsSecs[0]);
        int secs = Integer.parseInt(minsSecs[1]);
        return (mins * 60) + (secs);
    }

    public static Long getElapsedSeconds(Long beginTime) {
        return (System.currentTimeMillis() - beginTime) / 1000;
    }
    public static String secondsToString(Long beginTime) {
    	long Seconds =(System.currentTimeMillis() - beginTime) / 1000;
        return String.format("%02d:%02d", Seconds / 60, Seconds % 60);
    }

    
    
  public static void LOG(Long clock, String str)
    {
      System.out.println("[" + Utils.getElapsedSeconds(clock) + "] " + "<" + Thread.currentThread().getName() +">: " + str );
    }
   
    

}
