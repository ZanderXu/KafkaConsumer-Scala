package com.xuzq.MD5;

import java.security.MessageDigest;

/**
 * Created by xuzq on 2016/4/6.
 */
public class MessageMD5 {

    public MessageMD5(){

    }
    public String messageMD5(String message){
        char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        try {
            byte[] btInput = message.getBytes();
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            mdInst.update(btInput);
            byte[] md = mdInst.digest();
            int length = md.length;
            char str[] = new char[length * 2];
            int index = 0;
            for (int i = 0; i < length; i++) {
                byte byte0 = md[i];
                str[index++] = hexDigits[byte0 >>> 4 & 0xf];
                str[index++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public final static String MD5(String message) {
        char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        try {
            byte[] btInput = message.getBytes();
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            mdInst.update(btInput);
            byte[] md = mdInst.digest();
            int length = md.length;
            char str[] = new char[length * 2];
            int index = 0;
            for (int i = 0; i < length; i++) {
                byte byte0 = md[i];
                str[index++] = hexDigits[byte0 >>> 4 & 0xf];
                str[index++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public static void main(String[] args) {
        System.out.println(MessageMD5.MD5("20121221"));
        System.out.println(MessageMD5.MD5("加密"));
    }
}
