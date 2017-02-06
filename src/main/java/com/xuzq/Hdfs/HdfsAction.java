package com.xuzq.Hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsAction {

    private Configuration conf = null;

    public HdfsAction(){
        conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    public boolean isExist(FileSystem fs, String fileName) throws IOException {
        return  fs.exists(new Path(fileName));
    }

    public FSDataOutputStream create(FileSystem fs, String fileName) throws IOException{
        return fs.create(new Path(fileName));
    }

    public void writeContent(String fileName, byte[] buff){

        FileSystem fs  = null;
        try{
            fs = FileSystem.get(conf);

            if(!isExist(fs, fileName)){
                create(fs, fileName);
            }
            FSDataOutputStream out = fs.append(new Path(fileName));
            out.write(buff);
        }catch (IOException e){
            try{
                throw e;
            }catch (IOException e1){
                e1.printStackTrace();
            }
        }finally {
            try{
                fs.close();
            }catch (IOException e){
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws IOException {

        HdfsAction hdfsAction = new HdfsAction();

        String message = "123456";

        byte[] buff = message.getBytes();

        hdfsAction.writeContent("/home/spark/write.txt", buff);


    }
} 
