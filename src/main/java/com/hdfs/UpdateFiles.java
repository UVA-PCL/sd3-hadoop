package com.hdfs;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class UpdateFiles {
    public static void main(String[] args) throws IOException {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        System.out.println("executor created.");

        Cluster cluster;
        SD3Config.setLocalCluster(Integer.parseInt(args[0]));
        SD3Config.setClusterIPs(new String[]{args[1], args[2], args[3]});
        cluster = new Cluster(SD3Config.getLocalClusterIP());

        System.out.println("Starting at " + DateFormat.getInstance().format(new Date()));
        executor.scheduleAtFixedRate(new RunParseLog(Calendar.getInstance().getTime(), SD3Config.getReplicateHistoryInterval(), cluster, true),
                0, SD3Config.getReplicateInterval(), TimeUnit.SECONDS);

        Listener listener = new Listener(cluster);
        listener.start();
    }
}
