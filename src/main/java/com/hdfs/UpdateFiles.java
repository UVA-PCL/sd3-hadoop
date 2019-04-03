package com.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.concurrent.*;
import java.util.*;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileUtil;

public class UpdateFiles {
	public static void main(String[] args) throws IOException {
		if(args.length<1 || args.length>1) {
			System.out.println("argument number error");
			System.exit(0);
		}
		
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        System.out.println("executor created.");
        Cluster cluster = new Cluster(SD3Config.getHdfsRootFor(Integer.parseInt(args[0])));
        
        executor.scheduleAtFixedRate(new runParseLog(Calendar.getInstance().getTime(), 0, args,cluster,true),
				0, 3000, TimeUnit.SECONDS);
        
    }
}
