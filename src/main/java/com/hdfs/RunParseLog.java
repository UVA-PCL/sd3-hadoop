package com.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;


public class RunParseLog implements Runnable {
    private static final boolean DEBUG = true;

    Cluster cluster;
	
    private Date curTime;
    private int interval;
    public boolean usePolicy;
    
    
    RunParseLog(Date curTime, int interval, String[] args,Cluster c, boolean usePolicy){
        this.cluster = c;
    	this.curTime = curTime;
    	this.interval = interval;
    	this.usePolicy = usePolicy;
    	
    }
    
    public void run() {
        try {
            ArrayList<String[]> trimmed_records = null;
            double thold = 0.0;
            double replica_percent = 0.0;
            double total_file = 1000;
            double chosen_threshold = 0.0; 
            double chosen_replica_percent = 0.0;
            ArrayList<String[]> chosen_trimmed_records = new ArrayList<String[]>();
            if(usePolicy) {
                    System.out.println("The number of replicas over the threshold ranging from 0-100");
                    for(thold = 0.0;thold<=100; thold += 1.0 ) {
                            if(trimmed_records!=null&&trimmed_records.size()==0) {
                                break;
                            }

                            ParseLog pl = new ParseLog(SD3Config.getAuditLog(), curTime, interval);

                            System.out.println("About to read audit log");

                            pl.readFile();
                            
                            System.out.println("Done reading audit log");

                            ArrayList<String[]> records = pl.getFrequency(thold);
                            System.out.println("got " + records.size() + " records");

                            trimmed_records = trim(records);
                            System.out.println("got " + trimmed_records.size() + " trimmed records");
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
                    ParseLog pl = new ParseLog(SD3Config.getAuditLog(), curTime, interval);

                    pl.readFile();

                    ArrayList<String[]> records = pl.getFrequency(0);
                    trimmed_records = trim(records);
                    for(String[] item:trimmed_records) {
                                    chosen_trimmed_records.add(item);
                            }
            }
        
            System.out.println("about to update");	
            update(chosen_trimmed_records);
            System.out.println("about to get remote listeners");	
            InetSocketAddress[] listeners = SD3Config.getRemoteListeners();
            for (InetSocketAddress listener: listeners) {
                if (usePolicy) {
                    Helper.sendRequest(listener, "FINISH_PR");
                } else {
                    Helper.sendRequest(listener, "FINISH_AR");
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
        } catch (Exception e) {
            System.err.println("Failure in audit log parsing thread:");
            e.printStackTrace();
        }
    }
    /*
     * Trim the records that are done by the local cluster. The local cluster doesn't replicate local files.
     */
    public ArrayList<String[]> trim(ArrayList<String[]> records){
    	ArrayList<String[]> result = new ArrayList<String[]>();
    	for(String[] item:records) {
        	if(SD3Config.ipToClusterNumberOrZero(item[0]) == SD3Config.getLocalCluster()) {
        		
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
        String local_hdfs = SD3Config.getHdfsRootFor(SD3Config.getLocalCluster());
        for(String[] res : result){
            int remoteClusterNumber = SD3Config.ipToClusterNumberOrZero(res[0]);

            if (remoteClusterNumber == 0) {
                if (DEBUG) System.out.println("Unknown cluster for " + res[0]);
                continue;
            }
            
            String local_uri = local_hdfs + res[1];
            String remote_hdfs = SD3Config.getHdfsRootFor(remoteClusterNumber);
            String remote_uri = remote_hdfs + res[1];

            try {
                FileSystem local_fs = FileSystem.get(URI.create(local_uri), conf);
                FileSystem remote_fs = FileSystem.get(URI.create(remote_uri), conf);
                FileUtil.copy(local_fs, new Path(local_uri), remote_fs, new Path(remote_uri), false, conf);
                //System.out.println("Send file copy "+res[1]+ " from " + "local "+this.cluster.ip + " to "+cluster_name);
                String remoteRoot = SD3Config.getHdfsRootFor(remoteClusterNumber);
                synchronized(cluster.local_file) {
                	if(cluster.local_file.containsKey(res[1])) {
                		String clusters = cluster.local_file.get(res[1]);
                		clusters += "," + remoteRoot;
                		cluster.local_file.put(res[1], clusters);
                	}
                	else {
                		cluster.local_file.put(res[1], remoteRoot);
                	}               	
                }
                InetSocketAddress server = SD3Config.getListenerForCluster(remoteClusterNumber);
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
