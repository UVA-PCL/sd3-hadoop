package com.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class Deleter extends Thread{

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
			throw new RuntimeException(e);
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
			throw new RuntimeException(e);
		}
		System.out.println("delete file "+file_path);
		
	}
}
