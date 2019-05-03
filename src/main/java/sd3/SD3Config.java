package sd3;

import org.apache.hadoop.conf.Configuration;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SD3Config {
    private static final boolean DEBUG = true;
    private static final Map<String, Integer> ipToCluster = new HashMap<String, Integer>();
    private static final Map<Integer, String> clusterToIp = new HashMap<Integer, String>();
    private static int localCluster = 0;
    private static String filePrefix = "file/data/";
    private static String filePostfix = ".txt";
    private static int listenerPort = Integer.parseInt((String) System.getProperties().getOrDefault("sd3.listener-port", "22222"));
    private static int hdfsPort = Integer.parseInt((String) System.getProperties().getOrDefault("sd3.hdfs-port", "9000"));
    private static String traceDataRoot = System.getProperty("sd3.tracedata");
    private static String auditLog = System.getProperty("sd3.audit-log");
    private static int replicateInterval = Integer.parseInt(System.getProperty("sd3.replicate-interval", "10"));
    private static int replicateHistoryInterval = Integer.parseInt(System.getProperty("sd3.replicate-history-interval", "3600"));

    private static String hostToIp(String host) throws UnknownHostException {
        return InetAddress.getByName(host).toString().split("/")[1];
    }

    public static int getReplicateInterval() {
        return replicateInterval;
    }

    public static int getReplicateHistoryInterval() {
        return replicateHistoryInterval;
    }

    public static int getListenerPort() {
        return listenerPort;
    }

    public static String getTraceDataRoot() {
        return traceDataRoot;
    }

    public static String getAuditLog() {
        return auditLog;
    }

    public static InetSocketAddress getListenerForCluster(int id) {
        return Helper.createSocketAddress(clusterToIp.get(id) + ":" + listenerPort);
    }

    public static InetSocketAddress[] getRemoteListeners() {
        ArrayList<InetSocketAddress> result = new ArrayList<InetSocketAddress>();
        for (int id = 1; id <= ipToCluster.size(); ++id) {
            if (id == localCluster) continue;
            result.add(getListenerForCluster(id));
        }
        return result.toArray(new InetSocketAddress[0]);
    }

    public static void setLocalCluster(int id) {
        localCluster = id;
    }

    public static int getLocalCluster() {
        return localCluster;
    }

    public static String getLocalClusterIP() {
        return clusterToIp.get(localCluster);
    }

    public static void setClusterIPs(String[] hosts) throws UnknownHostException {
        int i = 1;
        for (String host : hosts) {
            String ip = hostToIp(host);
            if (DEBUG) System.out.println("cluster " + i + ": " + ip);
            ipToCluster.put(ip, i);
            clusterToIp.put(i, ip);
            ++i;
        }
    }

    public static int ipToClusterNumber(String ip) {
        if (ipToCluster.containsKey(ip)) {
            return ipToCluster.get(ip);
        } else {
            throw new RuntimeException("Could not lookup cluster for IP " + ip);
        }
    }

    public static int ipToClusterNumberOrZero(String ip) {
        if (ipToCluster.containsKey(ip)) {
            return ipToCluster.get(ip);
        } else {
            return 0;
        }
    }

    public static String filePath(String file) {
        return filePrefix + file + filePostfix;
    }

    public static String getHdfsRootFor(int clusterNumber) {
        return "hdfs://" + clusterToIp.get(clusterNumber) + ":" + hdfsPort + "/";
    }

    public static String getPathForOnCluster(int clusterNumber, String file) {
        return "hdfs://" + clusterToIp.get(clusterNumber) + ":" + hdfsPort + "/" + filePrefix + file + filePostfix;
    }

    public static String getLocalPathFor(String file) {
        return getPathForOnCluster(localCluster, file);
    }

    public static String getHomePathFor(String file) {
        return getPathForOnCluster(homeIdForFile(file), file);
    }

    public static int homeIdForFile(String path) {
        int lastSlash = path.lastIndexOf('/');
        String lastComponent = (lastSlash == -1) ? path : path.substring(lastSlash + 1);
        String idPart;
        if (lastComponent.startsWith("file")) {
            idPart = lastComponent.substring(4);
        } else {
            idPart = lastComponent;
        }
        return Integer.parseInt(idPart.substring(0, 1));
    }

    public static int getMaxClusterNumber() {
        return ipToCluster.size();
    }

    public static Configuration getHadoopConfig() {
        Configuration conf = new Configuration();

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.hds.impl.disable.cache", "true");

        return conf;
    }
}
