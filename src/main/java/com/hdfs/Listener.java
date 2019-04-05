package com.hdfs;

import java.io.IOException;
import java.net.InetAddress;
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

