package com.hdfs;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SD3Config {
    private static final Map<String, Integer> ipToCluster = new HashMap<String, Integer>();
    private static final Map<Integer, String> clusterToIp = new HashMap<Integer, String>();
    private static int localCluster = 0;
    private static String filePrefix = "file/data/";
    private static String filePostfix = ".txt";
    private static int listenerPort = Integer.parseInt((String) System.getProperties().getOrDefault("sd3.listener-port", "22222"));
    private static String traceDataRoot = System.getProperty("sd3.tracedata");
    private static String auditLog = System.getProperty("sd3.audit-log");

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
        for (int id = 1; id < ipToCluster.size(); ++id) {
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
    
    public static void setClusterIPs(String[] ips) {
        int i = 1;
        for (String ip : ips) {
            ipToCluster.put(ip, i);
            clusterToIp.put(i, ip);
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
        return "hdfs://" + clusterToIp.get(clusterNumber) + ":9000/";
    }

    public static String getPathForOnCluster(int clusterNumber, String file) {
        return "hdfs://" + clusterToIp.get(clusterNumber) + ":9000/" + filePrefix + file + filePostfix;
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
}
