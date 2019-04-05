package com.hdfs;

import java.util.HashMap;

//keep track of a cluster
public class Cluster {
	final HashMap<String, String> local_file = new HashMap<String,String>();
	final HashMap<String, String> remote_file = new HashMap<String,String>();
	int others__partial_replication_unfinish = 2;
	int others__all_replication_unfinish = 2;
	
	String ip = "";
	public Cluster(String ip){
		this.ip = ip;
	}
}
