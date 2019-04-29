package com.hdfs;

import java.util.HashMap;
import java.util.HashSet;

//keep track of a cluster
public class Cluster {
	final HashMap<String, String> local_file = new HashMap<String,String>();
	final HashMap<String, String> remote_file = new HashMap<String,String>();
	final HashSet<Integer> remote_finished = new HashSet<Integer>();
	final HashSet<Integer> remote_finished_pending = new HashSet<Integer>();

	public void finishRemote(int id) {
		synchronized(this) {
			if (remote_finished.contains(id)) {
				remote_finished_pending.add(id);
			} else {
				remote_finished.add(id);
				notify();
			}
		}
	}

	public void waitForRemotes() throws InterruptedException {
		synchronized(this) {
			while (remote_finished.size() < SD3Config.getMaxClusterNumber() - 1) {
				wait();
			}
			remote_finished.clear();
			remote_finished.addAll(remote_finished_pending);
		}
	}

	String ip = "";
	public Cluster(String ip){
		this.ip = ip;
	}
}
