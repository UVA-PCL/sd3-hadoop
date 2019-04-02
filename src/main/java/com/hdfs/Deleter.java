package com.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class Deleter extends Thread{

	final static String cluster1 = "128.143.69.231:9000";//shen-18 file 10-19
	final static String cluster2 = "128.143.69.230:9000";//shen-19 file 20-29
	final static String cluster3 = "128.143.69.229:9000";//shen-20 file 30-39
	String file_path;
	
	public Deleter(String path) {
		file_path = path;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void run() {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			 fs = FileSystem.get(URI.create(file_path),conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Path hdfs = new Path(file_path);
		conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
		try {
			FileUtil.fullyDelete(fs, hdfs);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("delete file "+file_path);
		
	}
	
	public static void main(String[] args) {
		new Deleter("hdfs://"+cluster1+"/file/data/file20.txt").start();
	}
	
}
