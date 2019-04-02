package com.hdfs;

import java.awt.*;
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

import javax.sound.midi.SysexMessage;


class runParseLog implements Runnable {
    //final static String filename = "/home/zl4dc/cls1/logs/hdfs-audit.log";
    final static String filename = System.getProperty("user.dir")+"/hadoop-2.7.7/logs/hdfs-audit.log";

    static String cluster1 = "128.143.69.231:9000";//shen-18 file 10-19
    static String cluster2 = "128.143.69.230:9000";//shen-19 file 20-29
    static String cluster3 = "128.143.69.229:9000";//shen-20 file 30-39
    //String cluster = "";
    static String localhost = "";

    Cluster cluster;
	
    private Date curTime;
    private int interval;
    public boolean usePolicy;
    
    
    runParseLog(Date curTime, int interval, String[] args,Cluster c, boolean usePolicy){
    	//this.cluster = cluster;
    	this.cluster = c;
    	cluster1 = args[2];
    	cluster2 = args[3];
    	cluster3 = args[4];
		if(args[0].equals("1")) {
			localhost = cluster1;
		}
		else if(args[0].equals("2")) {
			localhost = cluster2;
		}
		else {
			localhost = cluster3;
		}	
    	this.curTime = curTime;
    	this.interval = interval;
    	this.usePolicy = usePolicy;
    	
    }
    
    public void run() {
    	ArrayList<String[]> trimmed_records = null;
    	double thold = 0.0;
    	double replica_percent = 0.0;
    	double total_file = 1000;
    	double chosen_threshold = 0.0; 
    	double chosen_replica_percent = 0.0;
    	ArrayList<String[]> chosen_trimmed_records = new ArrayList<String[]>();
    	if(usePolicy) {
    		System.out.println("The number of replicas over the threshold ranging from 0-100");
    		for(thold = 0.0;thold<=100; thold++ ) {
    			if(trimmed_records!=null&&trimmed_records.size()==0) {
    				break;
    			}

    			ParseLog pl = new ParseLog(filename, curTime, interval);

    			pl.readFile();

    			ArrayList<String[]> records = pl.getFrequency(thold);

    			trimmed_records = trim(records);
    			replica_percent = trimmed_records.size()/total_file;
    			if(replica_percent >= 0.01 && replica_percent <= 0.03 && chosen_threshold == 0) {
    				chosen_threshold = thold;
    				chosen_replica_percent = replica_percent;
    				for(String[] item:trimmed_records) {
    					chosen_trimmed_records.add(item);
    				}
    			}
    			System.out.println(trimmed_records.size());
    		}
    		
    	System.out.println("chosen threshold = "+chosen_threshold+"\treplica percentage is "+chosen_replica_percent);
    	}
    	
    	else {
    		ParseLog pl = new ParseLog(filename, curTime, interval);

    		pl.readFile();

    		ArrayList<String[]> records = pl.getFrequency(0);
    		trimmed_records = trim(records);
    		for(String[] item:trimmed_records) {
				chosen_trimmed_records.add(item);
			}
    	}
    	
        update(chosen_trimmed_records);
        
        InetSocketAddress cl1 = Helper.createSocketAddress(cluster1.split(":")[0]+":22222");
		InetSocketAddress cl2 = Helper.createSocketAddress(cluster2.split(":")[0]+":22222");
		InetSocketAddress cl3 = Helper.createSocketAddress(cluster3.split(":")[0]+":22222");
        if(localhost.equals(cluster1)) {
			if(usePolicy) {
				Helper.sendRequest(cl2, "FINISH_PR");
				Helper.sendRequest(cl3, "FINISH_PR");
			}
			else {
				Helper.sendRequest(cl2, "FINISH_AR");
				Helper.sendRequest(cl3, "FINISH_AR");
			}
			
		}
		if(localhost.equals(cluster2)) {
			if(usePolicy) {
				Helper.sendRequest(cl1, "FINISH_PR");
				Helper.sendRequest(cl3, "FINISH_PR");
			}
			else {
				Helper.sendRequest(cl1, "FINISH_AR");
				Helper.sendRequest(cl3, "FINISH_AR");
			}
		}
		if(localhost.equals(cluster3)) {
			if(usePolicy) {
				Helper.sendRequest(cl1, "FINISH_PR");
				Helper.sendRequest(cl2, "FINISH_PR");
			}
			else {
				Helper.sendRequest(cl1, "FINISH_AR");
				Helper.sendRequest(cl2, "FINISH_AR");
			}
		}
		System.out.println("Send finish partial replication message to others. Waiting.");
        
        /*
        System.out.println("local file replica on the remote cluster");
        for(String file_path:cluster.local_file.keySet()) {
        	System.out.println(file_path+"\t"+cluster.local_file.get(file_path));
        }
        System.out.println("\n\n\nremote file replica on the local cluster");
        for(String file_path:cluster.remote_file.keySet()) {
        	System.out.println(file_path+"\t"+cluster.remote_file.get(file_path));
        }
        */
        
    }
    /*
     * Trim the records that are done by the local cluster. The local cluster doesn't replicate local files.
     */
    public ArrayList<String[]> trim(ArrayList<String[]> records){
    	ArrayList<String[]> result = new ArrayList<String[]>();
    	for(String[] item:records) {
        	if(item[0].equals(localhost.split(":")[0])) {
        		
        	}
        	else {
        		result.add(item);
        	}
        }
    	return result;
    }
    
    public void update(ArrayList<String[]> result ){

        Configuration conf = new Configuration();

        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        System.out.println("replicate files start");
        for(String[] res :result){
        	
        	String cluster_name = "shen-null";
            if(res[0].equals(cluster1.split(":")[0])) {
                //System.out.println("This operation was done by shen-18");
                cluster_name = "128.143.69.231:9000";
            }
            
            if(res[0].equals(cluster2.split(":")[0])) {
                //System.out.println("This operation was done by shen-19");
                cluster_name = "128.143.69.230:9000";
            }
            
            if(res[0].equals(cluster3.split(":")[0])) {
                //System.out.println("This operation was done by shen-20");
                cluster_name = "128.143.69.229:9000";
            }
            
            String local_uri = "hdfs://"+ localhost + res[1];
            String remote_uri = "hdfs://"+ cluster_name + res[1];

            try {
                FileSystem local_fs = FileSystem.get(URI.create(local_uri), conf);
                FileSystem remote_fs = FileSystem.get(URI.create(remote_uri), conf);
                FileUtil.copy(local_fs, new Path(local_uri), remote_fs, new Path(remote_uri), false, conf);
                //System.out.println("Send file copy "+res[1]+ " from " + "local "+this.cluster.ip + " to "+cluster_name);
                synchronized(cluster.local_file) {
                	if(cluster.local_file.containsKey(res[1])) {
                		String clusters = cluster.local_file.get(res[1]);
                		clusters += ","+cluster_name;
                		cluster.local_file.put(res[1], clusters);
                	}
                	else {
                		cluster.local_file.put(res[1], cluster_name);
                	}               	
                }
                InetSocketAddress server = Helper.createSocketAddress(cluster_name.split(":")[0]+":22222");
                String response = Helper.sendRequest(server, "COPY_"+res[1]);
                //System.out.println("response from "+cluster_name.split(":")[0]+" is "+response);
            }
            
            
            catch (Exception ex){
            	ex.printStackTrace();
                System.out.println("Fail to get FileSystem");
            }
        }
    }
    
}

class Helper{
	public static String sendRequest(InetSocketAddress server, String req) {
		if(server == null || req == null) {
			System.out.println("null server or request");
			return null;
		}
		
		Socket talkSocket = null;
		
		try {
			talkSocket = new Socket(server.getAddress(),server.getPort());
			PrintStream output = new PrintStream(talkSocket.getOutputStream());
			output.println(req);
		}
		catch(IOException e) {
			e.printStackTrace();
			return null;
		}
		try {
			Thread.sleep(60);
		}
		catch(InterruptedException e) {
			e.printStackTrace();
		}
		
		InputStream input = null;
		try {
			input = talkSocket.getInputStream();
		}
		catch(IOException e) {
			e.printStackTrace();
			System.out.println("Cannot get input stream from "+server.toString()+"\nRequest is: "+req+"\n");
		}
		String response = Helper.inputStreamToString(input);
		try {
			talkSocket.close();
		}
		catch(IOException e) {
			System.out.println("cannot close socket");
			e.printStackTrace();
		}
		return response;
	}
	
	public static InetSocketAddress createSocketAddress (String addr) {
		
		// input null, return null
		if (addr == null) {
			return null;
		}

		// split input into ip string and port string
		String[] splitted = addr.split(":");

		// can split string
		if (splitted.length >= 2) {

			//get and pre-process ip address string
			String ip = splitted[0];
			if (ip.startsWith("/")) {
				ip = ip.substring(1);
			}

			//parse ip address, if fail, return null
			InetAddress m_ip = null;
			try {
				m_ip = InetAddress.getByName(ip);
			} catch (UnknownHostException e) {
				System.out.println("Cannot create ip address: "+ip);
				return null;
			}

			// parse port number
			String port = splitted[1];
			int m_port = Integer.parseInt(port);

			// combine ip addr and port in socket address
			return new InetSocketAddress(m_ip, m_port);
		}

		// cannot split string
		else {
			return null;
		}

	}
	public static String inputStreamToString (InputStream in) {

		// invalid input
		if (in == null) {
			return null;
		}

		// try to read line from input stream
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line = null;
		try {
			line = reader.readLine();
		} catch (IOException e) {
			System.out.println("Cannot read line from input stream.");
			return null;
		}

		return line;
	}
}

public class UpdateFiles {
	final static String cluster1 = "128.143.69.231:9000";//shen-18 file 10-19
	final static String cluster2 = "128.143.69.230:9000";//shen-19 file 20-29
	final static String cluster3 = "128.143.69.229:9000";//shen-20 file 30-39
	public static void main(String[] args) throws IOException {
		if(args.length<1 || args.length>1) {
			System.out.println("argument number error");
			System.exit(0);
		}
		
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        System.out.println("executor created.");
        Cluster cluster;
        if(args[0].equals("1")) {
			cluster = new Cluster(cluster1);
		}
		else if(args[0].equals("2")) {
			cluster = new Cluster(cluster2);
		}
		else {
			cluster = new Cluster(cluster3);
		}	
        
        executor.scheduleAtFixedRate(new runParseLog(Calendar.getInstance().getTime(), 0, args,cluster,true),
				0, 3000, TimeUnit.SECONDS);
        
    }
}
