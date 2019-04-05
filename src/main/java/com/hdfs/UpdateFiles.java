package com.hdfs;

import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class UpdateFiles {
	public static void main(String[] args) throws IOException {
		if(args.length<1 || args.length>1) {
			System.out.println("argument number error");
			System.exit(0);
		}
		
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        System.out.println("executor created.");
        Cluster cluster = new Cluster(SD3Config.getHdfsRootFor(Integer.parseInt(args[0])));
        
        executor.scheduleAtFixedRate(new runParseLog(Calendar.getInstance().getTime(), 0, args,cluster,true),
				0, 3000, TimeUnit.SECONDS);
        
    }
}
