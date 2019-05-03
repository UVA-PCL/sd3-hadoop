package com.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class DeleteExtra {
    private static final boolean DEBUG = false;

    public static void clearExtraReplicasOnCluster(int clusterId) throws IOException {
        Configuration conf = SD3Config.getHadoopConfig();
        String allFilesPath = SD3Config.getPathForOnCluster(clusterId, "*");
        FileSystem fs = FileSystem.get(URI.create(allFilesPath), conf);
        for (FileStatus f : fs.globStatus(new Path(allFilesPath))) {
            if (DEBUG) System.out.println("Checking " + f.getPath());
            if (SD3Config.homeIdForFile(f.getPath().toString()) != clusterId) {
                fs.delete(f.getPath(), false);
                if (DEBUG) System.out.println("Deleted " + f.getPath());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        SD3Config.setClusterIPs(new String[]{args[0], args[1], args[2]});
        for (int i = 1; i <= SD3Config.getMaxClusterNumber(); ++i) {
            clearExtraReplicasOnCluster(i);
        }
    }
}
