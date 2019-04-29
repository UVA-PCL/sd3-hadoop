package com.hdfs;

import java.net.URI;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

public class DeleteExtra {
	public static void clearExtraReplicasOnCluster(int clusterId) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		String allFilesPath = SD3Config.getPathForOnCluster(clusterId, "*");
		FileSystem fs = FileSystem.get(URI.create(allFilesPath), conf);
		for (FileStatus f: fs.globStatus(new Path(allFilesPath))) {
			System.out.println("Checking " + f.getPath());
			if (SD3Config.homeIdForFile(f.getPath().toString()) != clusterId) {
				fs.delete(f.getPath(), false);
				System.out.println("Deleted " + f.getPath());
			}
		}
	}

	public static void main(String[] args) throws IOException {
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		System.out.println("executor created.");
		SD3Config.setClusterIPs(new String[]{args[0], args[1], args[2]});
		for (int i = 1; i <= SD3Config.getMaxClusterNumber(); ++i) {
			clearExtraReplicasOnCluster(i);
		}
	}
}
