package com.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import com.hdfs.ReadTrace.readFile;

public class Test {

	final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	final static String traceFile = System.getProperty("user.dir")+"/tracedata/tracedata.txt";
	final static String cluster1 = "128.143.69.231:9000";//shen-18 file 10-19
	final static String cluster2 = "128.143.69.230:9000";//shen-19 file 20-29
	final static String cluster3 = "128.143.69.229:9000";//shen-20 file 30-39
	final static String localhost = cluster1;
	
	static class writeFile implements Runnable {
		String cluster;
		String file;
		String cluster_number;

		writeFile(String cluster, String file, String cluster_number) {
			this.cluster = cluster;
			this.file = file;
			this.cluster_number = cluster_number;
		}

		@Override
		public void run() {
			
			
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			try {
				
				String original_cluster = "";
				if (file.startsWith("file1"))
					original_cluster = cluster1;
				else if (file.startsWith("file2"))
					original_cluster = cluster2;
				else
					original_cluster = cluster3;

			
					
				String uri = "hdfs://";
				uri= uri + cluster;
				uri = uri + "/file/data/";
				FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
				String uri2 = uri + file + ".txt";
				System.out.println("WRITE: " +uri2);
					
				String origin_uri = "hdfs://"+original_cluster + "/file/data/" + file+".txt" ;
				
				//try if the written file is on the local cluster
				
				InputStream in = null;
				FileSystem fs = FileSystem.get(URI.create(origin_uri), conf);
				in = fs.open(new Path(origin_uri));
				byte[] file_buffer = new byte[in.available()];
				in.read(file_buffer);
					
				OutputStream out = fs.create(new Path(origin_uri));
				IOUtils.copyBytes(in, out, 4096,true);
				hdfs.close();
				
				
			} catch (IOException ex) {
				//ex.printStackTrace();
				System.out.println("File "+file+" is not in the cluster");
				conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
				conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
				try {

					
					String uri = "hdfs://";
					uri= uri + cluster;
					uri = uri + "/file/data/";
					FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
					String uri2 = uri + file + ".txt";
					System.out.println("WRITE: " +uri2);

					InputStream in = null;
					FileSystem fs = FileSystem.get(URI.create(uri2), conf);
					in = fs.open(new Path(uri2));
					byte[] file_buffer = new byte[in.available()];
					in.read(file_buffer);
					
					OutputStream out = fs.create(new Path(uri2));
					IOUtils.copyBytes(in, out, 4096,true);
					hdfs.close();
				}
				catch (IOException ex2) {
					ex2.printStackTrace();
					System.out.println("cannot write file "+file);
				}
			}

		}
	}
	
	public static void main(String[] args) throws InterruptedException, IOException {
		Vector<Long> localLatencies = new Vector();
		Vector<Long> remoteLatencies = new Vector();
		long[] total_bandwidth = { 0, 0 };
		ExecutorService pool = Executors.newFixedThreadPool(1);
		//pool.execute(new writeFile(cluster1, "file10", "1") );//local write
		//pool.execute(new readFile("file20", total_bandwidth, localLatencies, remoteLatencies));//local read
		//pool.execute(new writeFile(cluster2, "file10", "2") );//remote write
		//pool.execute(new readFile("file10", total_bandwidth, localLatencies, remoteLatencies));//remote read
		//pool.shutdown();
		//pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
		
		/*
		String str = "2019-03-10 13:46:19,347 INFO FSNamesystem.audit: allowed=true	ugi=zl4dc (auth:SIMPLE)	ip=/128.143.69.229	cmd=open	src=/file/data/file32.txt	dst=null	perm=null	proto=rpc";      
		String[] p = str.split("\t| ");
		for(int i=0;i<p.length;i++) {
			System.out.println("Entry "+(i+1)+" is: "+p[i]);
		}
		*/
		
		/*
		String filename ="/home/zl4dc/sharefolder/Data-Replication/hdfs-audit.log";
		ParseLog pl = new ParseLog(filename, Calendar.getInstance().getTime(), 20);
        System.out.println("Parse Log");
        pl.readFile();
        //pl.printAll();
        ArrayList<String[]> records = pl.getFrequency(3.0);
        System.out.println("print result");
        System.out.println(records.size());
        for(String[] item:records) {
        	System.out.println(item[0]+" "+item[1]);
        }
        */
		
		
		/*
		Listener listener = new Listener(new Cluster(cluster1));
		listener.start();
		InetSocketAddress server = Helper.createSocketAddress("10.0.2.15:22222");
		Helper.sendRequest(server, "ELSE");
		listener.toDie();
		*/
		/*
		String a = "abc";
		String[] as = a.split(",");
		for(int i=0;i<as.length;i++)
			System.out.println(as[i]);
		*/
		
		
	}

}
