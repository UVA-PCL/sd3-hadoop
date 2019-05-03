package sd3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class Speaker implements Runnable {
    private static final boolean DEBUG = false;

    Socket talkSocket;
    Cluster cluster;

    public Speaker(Socket _talkSocket, Cluster c) {
        cluster = c;
        talkSocket = _talkSocket;

    }


    public void run() {
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

    private String processRequest(String request) throws IOException {
        if (DEBUG) System.out.println("Speaker got request " + request);
        String ret = null;

        String original_cluster_ip = talkSocket.getInetAddress().toString().split("/")[1];
        int cluster_id = SD3Config.ipToClusterNumberOrZero(original_cluster_ip);
        if (cluster_id == 0) {
            System.err.println("Received message from unknown node " + original_cluster_ip);
        }
        if (request == null) {
            return null;
        }
        if (request.startsWith("COPY")) {
            String file_path = request.split("_")[1];
            synchronized (cluster.remote_file) {
                cluster.remote_file.put(file_path, cluster_id);//record originial cluster of the replica
                //file_path is like "/file/data/filexx.txt"
            }
            //System.out.println("record file replica " + file_path + ", original cluster "+original_cluster_ip);
            ret = "GET COPY";
        } else if (request.startsWith("DELETECOPY")) {
            String file_path = request.split("_")[1];
            //System.out.println("message from "+talkSocket.getInetAddress().toString().split("/")[1]);
            synchronized (cluster.local_file) {
                if (cluster.local_file.containsKey(file_path)) {
                    //System.out.println("delete copy");
                    String clusters_str = cluster.local_file.get(file_path);
                    String[] replica_clusters = clusters_str.split(",");
                    for (String replica_cluster_number_str : replica_clusters) {
                        //System.out.println("delete "+file_path+" copy on "+replica_clusters[i]);
                        int replica_cluster_number = Integer.parseInt(replica_cluster_number_str);
                        new Deleter(SD3Config.getHdfsRootFor(replica_cluster_number) + file_path).start();
                        Helper.sendRequest(SD3Config.getListenerForCluster(replica_cluster_number), "UPDATE_REMOTE_FILE," + file_path);
                    }
                    cluster.local_file.remove(file_path);
                    ret = file_path + "DELETE SUCCESSFULLY";
                } else {
                    ret = file_path + "NO REPLICA";
                }
            }
        } else if (request.startsWith("UPDATE_REMOTE_FILE")) {
            String file_path = request.split(",")[1];
            synchronized (cluster.remote_file) {
                cluster.remote_file.remove(file_path);
            }

        } else if (request.startsWith("FINISH,")) {
            int id = Integer.parseInt(request.split(",")[1]);
            cluster.finishRemote(id);
        }


        return ret;
    }
}
