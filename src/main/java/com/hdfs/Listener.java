package com.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;


public class Listener extends Thread {

	Cluster cluster;
	private ServerSocket serverSocket;
	private boolean alive;

	public Listener (Cluster c) {		
		cluster = c;
		alive = true;
		
		String localip ="";
		try {
			localip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String port = "22222";

		

		//open server/listener socket
		try {
			serverSocket = new ServerSocket(Integer.parseInt(port));
		} catch (IOException e) {
			throw new RuntimeException("\nCannot open listener port "+port+". Now exit.\n", e);
		}
	}

	@Override
	public void run() {
		while (alive) {
			Socket talkSocket = null;
			try {
				talkSocket = serverSocket.accept();
			} catch (IOException e) {
				throw new RuntimeException(
						"Cannot accepting connection", e);
			}

			//new talker
			new Thread(new Speaker(talkSocket,cluster)).start();
		}
	}

	public void toDie() {
		alive = false;
	}
}

class Speaker implements Runnable{

	Socket talkSocket;
	Cluster cluster;

	public Speaker(Socket _talkSocket, Cluster c)
	{
		cluster = c;
		talkSocket = _talkSocket;
		
	}
	
	
	
	public void run()
	{
		InputStream input = null;
		OutputStream output = null;
		try {
			input = talkSocket.getInputStream();
			String request = Helper.inputStreamToString(input);
			String response = processRequest(request);
			if (response != null) {
				output = talkSocket.getOutputStream();
				output.write(response.getBytes());
			}
			input.close();
		} catch (IOException e) {
			throw new RuntimeException("Cannot speak", e);
		}
	}

	private String processRequest(String request) throws IOException
	{
		InetSocketAddress result = null;
		String ret = null;
		
		String original_cluster_ip = talkSocket.getInetAddress().toString().split("/")[1];
		if (request  == null) {
			return null;
		}
		if (request.startsWith("COPY")) {
			String file_path = request.split("_")[1];
			synchronized(cluster.remote_file) {
				cluster.remote_file.put(file_path, original_cluster_ip+":9000");//record originial cluster of the replica
				//file_path is like "/file/data/filexx.txt"
			}
			//System.out.println("record file replica " + file_path + ", original cluster "+original_cluster_ip);
			ret = "GET COPY";
		}
		else if(request.startsWith("DELETECOPY")) {
			String file_path = request.split("_")[1];
			//System.out.println("message from "+talkSocket.getInetAddress().toString().split("/")[1]);
			synchronized(cluster.local_file) {
				if(cluster.local_file.containsKey(file_path)) {
					//System.out.println("delete copy");
					String clusters_str = cluster.local_file.get(file_path);
					String[] replica_clusters = clusters_str.split(",");
					for(int i=0;i<replica_clusters.length;i++) {
						//System.out.println("delete "+file_path+" copy on "+replica_clusters[i]);
						new Deleter("hdfs://"+replica_clusters[i]+file_path).start();;
						InetSocketAddress server = Helper.createSocketAddress(replica_clusters[i].split(":")[0]+":22222");
						Helper.sendRequest(server, "UPDATE_REMOTE_FILE,"+file_path);
					}
					cluster.local_file.remove(file_path);
					ret = file_path+"DELETE SUCCESSFULLY";
				}
				else {
					ret = file_path+"NO REPLICA";
				}
			}
		}
		else if(request.startsWith("UPDATE_REMOTE_FILE")) {
			String file_path = request.split(",")[1];
			synchronized(cluster.remote_file) {
				cluster.remote_file.remove(file_path);
			}
			
		}
		else if(request.startsWith("FINISH_PR")) {
			cluster.others__partial_replication_unfinish--;
		}
		else if(request.startsWith("FINISH_AR")) {
			cluster.others__all_replication_unfinish--;
		}
		
		
		return ret;
	}
}