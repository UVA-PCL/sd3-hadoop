package com.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;


public class Listener extends Thread {
        private static final boolean DEBUG = true;

	Cluster cluster;
	private ServerSocket serverSocket;
	private boolean alive;
        public Listener(Cluster c) {
		cluster = c;
		alive = true;
		
		//open server/listener socket
		try {
			serverSocket = new ServerSocket();
                        serverSocket.setReuseAddress(true);
                        serverSocket.bind(new InetSocketAddress(SD3Config.getListenerPort()));
		} catch (IOException e) {
			throw new RuntimeException("\nCannot open listener port "+SD3Config.getListenerPort()+". Now exit.\n", e);
		}
                if (DEBUG) System.out.println("Created listener socket " + serverSocket);
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
                        if (DEBUG) System.out.println("Creating Speaker thread");

			//new talker
			new Thread(new Speaker(talkSocket,cluster)).start();
		}
	}

	public void toDie() {
		alive = false;
	}
}

